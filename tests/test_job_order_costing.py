import tempfile
import unittest
from pathlib import Path

from job_order_costing import JobOrderCostingSystem


class TestJobOrderCosting(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "test.db"
        self.app = JobOrderCostingSystem(self.db)
        self.app.init_db()

    def tearDown(self):
        self.tmp.cleanup()

    def test_cost_formula(self):
        self.app.create_order("O1", "C", "W", "P", "2026-01-01", "opened", "labor_hours", "v1", "u", price=1000, plan_cost=700)
        self.app.add_bom("O1", "M1", 2, 100, "u")
        self.app.add_labor("O1", "E1", 3, 50, "u")
        self.app.add_overhead("O1", 120, "energy", "u")
        b = self.app.calculate_order_cost("O1")
        self.assertEqual(b.materials, 200)
        self.assertEqual(b.labor, 150)
        self.assertEqual(b.overhead, 120)
        self.assertEqual(b.total_cost, 470)
        self.assertEqual(b.margin, 530)

    def test_close_requires_all_cost_types(self):
        self.app.create_order("O2", "C", "W", "P", "2026-01-01", "opened", "labor_hours", "v1", "u")
        self.app.update_order_status("O2", "in_progress", "u")
        self.app.add_bom("O2", "M1", 1, 10, "u")
        self.app.add_labor("O2", "E1", 1, 10, "u")
        with self.assertRaises(ValueError):
            self.app.update_order_status("O2", "completed", "u")


if __name__ == "__main__":
    unittest.main()
