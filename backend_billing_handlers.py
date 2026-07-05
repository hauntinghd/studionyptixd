"""Billing route handlers for the Studio API."""

from fastapi import Depends, HTTPException, Request


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
    billing_stripe_primary: bool,
    before_trial_checkout=None,
    create_stripe_membership_checkout,
    paypal_enabled,
    create_paypal_subscription_order,
):
    async def create_checkout(
        req: checkout_request_model,
        request: Request = None,
        user: dict = Depends(require_auth),
    ):
        requested_product = str(getattr(req, "product", "") or "").strip().lower()
        requested_plan = str(getattr(req, "plan", "") or "").strip().lower()
        requested_trial = bool(getattr(req, "trial", False))
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
        if requested_trial and before_trial_checkout:
            before_trial_checkout(user, plan, request)
        if stripe_secret_key and billing_stripe_primary:
            checkout_url = await create_stripe_membership_checkout(user, plan, price_usd, requested_trial, request)
            return {"checkout_url": checkout_url, "provider": "stripe"}
        if requested_trial:
            raise HTTPException(400, "Free trial requires card checkout through Stripe.")
        if paypal_enabled():
            checkout_url = await create_paypal_subscription_order(user, price_id, plan, price_usd)
            return {"checkout_url": checkout_url, "provider": "paypal"}
        if stripe_secret_key:
            checkout_url = await create_stripe_membership_checkout(user, plan, price_usd, requested_trial, request)
            return {"checkout_url": checkout_url, "provider": "stripe"}
        raise HTTPException(503, "No payment provider configured")

    return create_checkout
