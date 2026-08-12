import base64
import json
import logging
import secrets
import time
from contextlib import asynccontextmanager
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse

from .config import settings
from .keycloak_client import KeycloakClient, build_authorization_url, generate_pkce_pair
from .profiles import ProfileStore
from .sessions import SessionStore, token_expired

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bionicpro-auth")

store = SessionStore()
keycloak = KeycloakClient()
profiles = ProfileStore()


@asynccontextmanager
async def lifespan(_: FastAPI):
    await profiles.connect()
    yield
    await keycloak.close()
    await store.close()
    await profiles.close()


app = FastAPI(title="bionicpro-auth", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Session-Rotated"],
)


def decode_claims(token: str) -> dict:
    """Читаем payload access_token. Подпись проверяет ресурсный сервис по JWKS,
    здесь токен пришёл напрямую от Keycloak по серверному каналу."""
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return json.loads(base64.urlsafe_b64decode(payload))
    except Exception:
        return {}


def set_session_cookie(response: Response, session_id: str) -> None:
    response.set_cookie(
        key=settings.session_cookie,
        value=session_id,
        max_age=settings.session_ttl,
        httponly=True,
        secure=settings.cookie_secure,
        samesite=settings.cookie_samesite,
        domain=settings.cookie_domain,
        path="/",
    )


def session_payload(tokens: dict) -> dict:
    claims = decode_claims(tokens["access_token"])
    return {
        "access_token": tokens["access_token"],
        "refresh_token": tokens.get("refresh_token"),
        "access_expires_at": time.time() + int(tokens.get("expires_in", 60)),
        "sub": claims.get("sub"),
        "username": claims.get("preferred_username"),
        "email": claims.get("email"),
        "name": claims.get("name"),
        "roles": claims.get("realm_access", {}).get("roles", []),
        "identity_provider": claims.get("identity_provider", "keycloak"),
        "created_at": time.time(),
    }


async def load_session(request: Request) -> tuple[str, dict]:
    session_id = request.cookies.get(settings.session_cookie, "")
    session = await store.get(session_id)
    if not session:
        raise HTTPException(status_code=401, detail="Сессия не найдена или истекла")

    if token_expired(session):
        if not session.get("refresh_token"):
            await store.delete(session_id)
            raise HTTPException(status_code=401, detail="Сессия истекла")
        try:
            tokens = await keycloak.refresh(session["refresh_token"])
        except httpx.HTTPError:
            await store.delete(session_id)
            raise HTTPException(status_code=401, detail="Не удалось обновить токен")
        # часть данных сессии переживает обновление токенов
        refreshed = session_payload(tokens)
        refreshed["consent_granted"] = session.get("consent_granted", False)
        session = refreshed
        await store.update(session_id, session)
        log.info("access_token обновлён по refresh_token, пользователь %s", session.get("username"))

    return session_id, session


async def finish(request: Request, session_id: str, session: dict, response: Response) -> Response:
    """Ротация сессии на каждом обращении к защищённому ресурсу."""
    if not settings.rotate_session:
        set_session_cookie(response, session_id)
        return response
    new_session_id = await store.rotate(session_id, session)
    set_session_cookie(response, new_session_id)
    response.headers["X-Session-Rotated"] = "1"
    return response


@app.get("/health")
async def health():
    return {"status": "ok", "redis": await store.ping()}


@app.get("/auth/login")
async def login(redirect_to: Optional[str] = None):
    state = secrets.token_urlsafe(24)
    nonce = secrets.token_urlsafe(16)
    verifier, challenge = generate_pkce_pair()
    await store.save_login_flow(
        state,
        {
            "code_verifier": verifier,
            "nonce": nonce,
            "redirect_to": redirect_to or settings.frontend_url,
        },
    )
    return RedirectResponse(build_authorization_url(state, challenge, nonce), status_code=302)


@app.get("/auth/callback")
async def callback(request: Request, code: Optional[str] = None, state: Optional[str] = None,
                   error: Optional[str] = None):
    if error:
        return RedirectResponse(f"{settings.frontend_url}/?error={error}", status_code=302)
    if not code or not state:
        raise HTTPException(status_code=400, detail="Отсутствует code или state")

    flow = await store.pop_login_flow(state)
    if flow is None:
        raise HTTPException(status_code=400, detail="Неизвестный или истёкший state")

    try:
        tokens = await keycloak.exchange_code(code, flow["code_verifier"])
    except httpx.HTTPStatusError as exc:
        log.warning("Обмен кода на токен не удался: %s", exc.response.text)
        raise HTTPException(status_code=401, detail="Не удалось получить токены")

    session = session_payload(tokens)
    # если профиль ещё не сохранён, спросим у пользователя согласие на обработку данных
    session["consent_granted"] = await profiles.exists(session["sub"] or "")

    session_id = await store.create(session)
    target = flow.get("redirect_to") or settings.frontend_url
    if not session["consent_granted"]:
        target = f"{target}/?consent=1"

    response = RedirectResponse(target, status_code=302)
    set_session_cookie(response, session_id)
    log.info("Пользователь %s вошёл в систему", session.get("username"))
    return response


@app.get("/auth/session")
async def current_session(request: Request):
    session_id, session = await load_session(request)
    body = {
        "authenticated": True,
        "username": session.get("username"),
        "name": session.get("name"),
        "email": session.get("email"),
        "roles": session.get("roles", []),
        "identityProvider": session.get("identity_provider"),
        "consentGranted": session.get("consent_granted", False),
    }
    return await finish(request, session_id, session, JSONResponse(body))


@app.post("/auth/consent")
async def consent(request: Request):
    session_id, session = await load_session(request)
    payload = {}
    try:
        payload = await request.json()
    except Exception:
        pass

    if not payload.get("granted"):
        await store.delete(session_id)
        await keycloak.logout(session.get("refresh_token"))
        response = JSONResponse({"consentGranted": False})
        response.delete_cookie(settings.session_cookie, path="/")
        return response

    # Данные профиля берём из access_token: туда их кладёт Keycloak,
    # в том числе то, что пришло от внешнего провайдера (Яндекс ID).
    claims = {
        "sub": session.get("sub"),
        "preferred_username": session.get("username"),
        "email": session.get("email"),
        "name": session.get("name"),
    }
    await profiles.upsert(session["sub"], claims, session.get("identity_provider", "keycloak"))
    session["consent_granted"] = True
    await store.update(session_id, session)
    return await finish(request, session_id, session, JSONResponse({"consentGranted": True}))


@app.post("/auth/logout")
async def logout(request: Request):
    session_id = request.cookies.get(settings.session_cookie, "")
    session = await store.get(session_id)
    if session:
        await keycloak.logout(session.get("refresh_token"))
        await store.delete(session_id)
    response = JSONResponse({"loggedOut": True})
    response.delete_cookie(settings.session_cookie, path="/")
    return response


@app.api_route("/api/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def proxy(request: Request, path: str):
    """Единая точка входа для фронтенда: наружу — сессионная cookie, внутрь — Bearer-токен."""
    session_id, session = await load_session(request)
    if not session.get("consent_granted", False):
        raise HTTPException(status_code=403, detail="Требуется согласие на обработку данных")

    url = f"{settings.reports_api_url}/{path}"
    async with httpx.AsyncClient(timeout=30.0) as client:
        upstream = await client.request(
            request.method,
            url,
            params=request.query_params,
            content=await request.body(),
            headers={
                "Authorization": f"Bearer {session['access_token']}",
                "Accept": request.headers.get("accept", "application/json"),
                "Content-Type": request.headers.get("content-type", "application/json"),
            },
        )

    response = Response(
        content=upstream.content,
        status_code=upstream.status_code,
        media_type=upstream.headers.get("content-type"),
    )
    return await finish(request, session_id, session, response)
