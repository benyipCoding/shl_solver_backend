import unittest

from pydantic import ValidationError

from app.prompts.excel_workbench import generate_prompt
from app.schemas.excel_workbench import TransformRequest, AIResponseSchema


class ExcelWorkbenchProtocolTests(unittest.TestCase):
    def test_legacy_clients_keep_array_protocol(self):
        payload = TransformRequest(prompt="清理空格", columns=["姓名"])
        self.assertEqual(payload.protocol_version, 1)
        self.assertIn("transform(data)", generate_prompt(payload.columns, {}))
        self.assertNotIn("OUTPUT protocol v2", generate_prompt(payload.columns, {}))

    def test_v2_includes_formatting_identity_and_sparse_data_contract(self):
        payload = TransformRequest(prompt="背景红色并加粗", columns=["销售额"], protocol_version=2)
        prompt = generate_prompt(payload.columns, {"销售额": 12000}, payload.protocol_version)
        for required in ["sourceRows", "context.sourceIndex", "backgroundColor", "bold", "headerCells", "numberFormat", "styles: null", "Rows may be sparse", "sort/filter FIRST"]:
            self.assertIn(required, prompt)
        self.assertIn('"销售额": 12000', prompt)

    def test_unknown_protocol_and_invalid_ai_results_fail_validation(self):
        with self.assertRaises(ValidationError):
            TransformRequest(prompt="x", columns=[], protocol_version=3)
        with self.assertRaises(ValidationError):
            AIResponseSchema.model_validate_json('{"explanation":"missing code"}')


if __name__ == "__main__":
    unittest.main()
