"""Billing route handlers for the Studio API."""

from fastapi import Depends, HTTPException


def build_create_checkout_handler(
    *,
    checkout_request_model,
    require_auth,
    default_membership_plan_id,
    price_id_for_plan_id,
    stripe_price_to_plan: dict,
    unified_plans: dict,
    plan_price_usd: dict,
    chat_story_allowed_plans: set[str],
    stripe_secret_key: str,
    create_stripe_membership_checkout,
):
    async def create_checkout(req: checkout_request_model, user: dict = Depends(require_auth)):
        requested_product = str(getattr(req, "product", "") or "").strip().lower()
        requested_plan = str(getattr(req, "plan", "") or "").strip().lower()
        price_id = str(req.price_id or "").strip()
        if requested_product == "membership" and not requested_plan and not price_id:
            requested_plan = default_membership_plan_id()
        if requested_plan and not price_id:
            price_id = price_id_for_plan_id(requested_plan)
        plan = str(stripe_price_to_plan.get(price_id, requested_plan) or "").strip().lower()
        if plan in unified_plans:
            price_usd = float(unified_plans[plan]["price_usd"])
        else:
            price_usd = float(plan_price_usd.get(plan, 0.0) or 0.0)
        if plan not in chat_story_allowed_plans:
            raise HTTPException(400, "This membership plan is not available for checkout.")
        if price_usd <= 0:
            raise HTTPException(400, f"Membership pricing is not configured for {plan}.")
        if not stripe_secret_key:
            raise HTTPException(503, "Stripe checkout is temporarily unavailable.")
        checkout_url = await create_stripe_membership_checkout(user, plan, price_usd)
        return {"checkout_url": checkout_url, "provider": "stripe"}

    return create_checkout
