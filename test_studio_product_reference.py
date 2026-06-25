import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from long_form.pipeline import compute_render_cost
from long_form.prompts.channels import get_channel
from studio_agent import product_reference


class ProductReferenceTests(unittest.TestCase):
    def test_private_product_url_is_rejected(self):
        with patch("studio_agent.product_reference.socket.getaddrinfo") as lookup:
            lookup.return_value = [(None, None, None, None, ("127.0.0.1", 443))]
            with self.assertRaises(product_reference.ProductReferenceError):
                product_reference._assert_public_url("https://example.test/product")

    def test_attached_product_image_creates_durable_manifest(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            image = root / "product.png"
            image.write_bytes(b"\x89PNG\r\n\x1a\n" + b"x" * 2048)
            reference_root = root / "references"
            output_root = root
            with (
                patch.object(product_reference, "OUTPUT_ROOT", output_root),
                patch.object(product_reference, "REFERENCE_ROOT", reference_root),
            ):
                manifest = product_reference.ingest(
                    session_id="session",
                    user_id="user",
                    attached_paths=[str(image)],
                    product_name="Example Product",
                )
                loaded = product_reference.load(manifest["reference_id"], user_id="user")
            self.assertEqual(loaded["product_name"], "Example Product")
            self.assertEqual(len(loaded["images"]), 1)


class LongFormMotionBudgetTests(unittest.TestCase):
    def test_balanced_motion_only_prices_hero_scenes(self):
        outline = {
            "chapters": [{"title": "A"}, {"title": "B"}, {"title": "C"}],
            "motion_policy": "balanced",
        }
        cost = compute_render_cost(get_channel("empire_magnates"), outline)
        self.assertEqual(cost["n_scenes"], 36)
        self.assertEqual(cost["animated_scenes"], 13)
        self.assertEqual(cost["still_motion_scenes"], 23)
        self.assertLess(cost["breakdown"]["ltx_i2v_clips"], 36 * 0.04)


if __name__ == "__main__":
    unittest.main()
