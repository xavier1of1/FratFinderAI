from pathlib import Path

from fratfinder_crawler.adapters.directory_v1 import DirectoryV1Adapter


def test_directory_adapter_extracts_cards():
    fixture = Path("services/crawler/fixtures/sample_directory.html").read_text(encoding="utf-8")

    records = DirectoryV1Adapter().parse(fixture, "https://example.org/chapters")

    assert len(records) == 2
    assert records[0].name == "Beta Lambda"
    assert records[0].city == "Columbus"
    assert records[0].state == "OH"
    assert records[1].website_url is None


def test_directory_adapter_skips_table_headers_and_reads_city_state_columns():
    fixture = """
    <table>
      <tr>
        <th>Greek-letter Chapter Name</th>
        <th>College/University</th>
        <th>City</th>
        <th>State/Province</th>
        <th>Country</th>
      </tr>
      <tr>
        <td>Alpha</td>
        <td>Miami University</td>
        <td>Oxford</td>
        <td>OH</td>
        <td>United States</td>
      </tr>
    </table>
    """

    records = DirectoryV1Adapter().parse(fixture, "https://example.org/chapters")

    assert len(records) == 1
    assert records[0].name == "Alpha"
    assert records[0].university_name == "Miami University"
    assert records[0].city == "Oxford"
    assert records[0].state == "OH"
