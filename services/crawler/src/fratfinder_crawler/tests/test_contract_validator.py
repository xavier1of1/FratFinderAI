from fratfinder_crawler.contracts import ContractValidator


def test_contract_validator_accepts_valid_chapter_payload():
    validator = ContractValidator()

    validator.validate_chapter(
        {
            "fraternitySlug": "beta-theta-pi",
            "sourceSlug": "beta-theta-pi-main",
            "externalId": None,
            "slug": "beta-lambda",
            "name": "Beta Lambda",
            "universityName": "Ohio State University",
            "city": "Columbus",
            "state": "OH",
            "country": "USA",
            "websiteUrl": "https://example.org/beta-lambda",
            "chapterStatus": "active",
            "missingOptionalFields": []
        }
    )