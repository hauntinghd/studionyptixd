"""Router mounting helpers for the Studio FastAPI app."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.routing import APIRouter


def mount_router(app: FastAPI, router: APIRouter) -> APIRouter:
    """Register a router and return it for smoke-test introspection."""
    app.include_router(router)
    return router
