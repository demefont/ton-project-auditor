from __future__ import annotations

import unittest

from identity_validator.utils import detect_contract_paths


class ContractDetectionTests(unittest.TestCase):
    def test_docs_images_are_not_detected_as_contracts(self) -> None:
        paths = [
            "docs/images/1collection.png",
            "docs/images/nft-metadata.jpg",
            "packages/contracts/nft-item/NftItem.source.ts",
            "packages/contracts/sources/nft-collection.fc",
            "wrappers/JettonMinter.ts",
        ]
        detected = detect_contract_paths(paths)
        self.assertNotIn("docs/images/1collection.png", detected)
        self.assertNotIn("docs/images/nft-metadata.jpg", detected)
        self.assertIn("packages/contracts/nft-item/NftItem.source.ts", detected)
        self.assertIn("packages/contracts/sources/nft-collection.fc", detected)
        self.assertIn("wrappers/JettonMinter.ts", detected)


if __name__ == "__main__":
    unittest.main()
