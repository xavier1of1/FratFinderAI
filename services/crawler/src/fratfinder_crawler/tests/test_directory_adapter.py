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


def test_directory_adapter_extracts_elementor_archive_contact_entries():
    fixture = """
    <article>
      <section>
        <h2>MISSISSIPPI STATE CHAPTER</h2>
        <h2>PO Box GK Mississippi State Mississippi State, MS 39762</h2>
        <div class="elementor-widget-text-editor">
          <div class="elementor-widget-container">
            Website: <a href="http://msstatedeltachi.com/">Delta Chi Mississippi State</a>
            Instagram: <a href="https://www.instagram.com/msstatedeltachi">@msstatedeltachi</a>
          </div>
        </div>
      </section>
    </article>
    """

    records = DirectoryV1Adapter().parse(fixture, "https://deltachi.org/chapter-directory/mississippi/")
    stubs = DirectoryV1Adapter().parse_stubs(fixture, "https://deltachi.org/chapter-directory/mississippi/")

    assert len(records) == 1
    assert records[0].name == "Mississippi State Chapter"
    assert records[0].university_name == "Mississippi State"
    assert records[0].website_url == "http://msstatedeltachi.com/"
    assert records[0].instagram_url == "https://www.instagram.com/msstatedeltachi"
    assert len(stubs) == 1
    assert stubs[0].chapter_name == "Mississippi State Chapter"
    assert stubs[0].provenance == "directory_v1:archive_entry"

def test_directory_adapter_extracts_bootstrap_chapter_cards_and_splits_university_name():
    fixture = """
    <div class=\"grid-item\">
      <div class=\"card h-100\">
        <div class=\"card-body\">
          <h3 class=\"card-title\">
            <a href=\"https://www.kdr.com/chapter/beta-cornell-university/\">Beta - Cornell University</a>
          </h3>
        </div>
      </div>
    </div>
    """

    records = DirectoryV1Adapter().parse(fixture, "https://www.kdr.com/chapters/")
    stubs = DirectoryV1Adapter().parse_stubs(fixture, "https://www.kdr.com/chapters/")

    assert len(records) == 1
    assert records[0].name == "Beta"
    assert records[0].university_name == "Cornell University"
    assert records[0].website_url == "https://www.kdr.com/chapter/beta-cornell-university/"
    assert len(stubs) == 1
    assert stubs[0].chapter_name == "Beta"
    assert stubs[0].university_name == "Cornell University"



def test_directory_adapter_uses_header_aware_table_columns_for_chi_psi_style_tables():
    fixture = """
    <table>
      <tr>
        <th>ALPHA</th>
        <th>SYMBOL</th>
        <th>COLLEGE</th>
        <th>FOUNDED</th>
      </tr>
      <tr>
        <td>Pi</td>
        <td>&Pi;</td>
        <td>Union College</td>
        <td>1841</td>
      </tr>
    </table>
    """

    records = DirectoryV1Adapter().parse(fixture, "https://chipsi.org/where-we-are/")
    stubs = DirectoryV1Adapter().parse_stubs(fixture, "https://chipsi.org/where-we-are/")

    assert len(records) == 1
    assert records[0].name == "Pi"
    assert records[0].university_name == "Union College"
    assert len(stubs) == 1
    assert stubs[0].chapter_name == "Pi"
    assert stubs[0].university_name == "Union College"


def test_directory_adapter_extracts_repeated_list_chapter_entries():
    fixture = """
    <section>
      <h2>Theta Xi Chapters</h2>
      <ul>
        <li><span>Auburn University – Beta Zeta Chapter</span></li>
        <li><span>University of Alabama – Alpha Lambda Chapter</span></li>
        <li><span>Arizona State University – Delta Alpha Chapter</span></li>
        <li><span>Embry-Riddle Aeronautical University – Gamma Iota Chapter</span></li>
        <li><span>University of Arizona – Gamma Psi Chapter</span></li>
      </ul>
    </section>
    """

    records = DirectoryV1Adapter().parse(fixture, "https://www.thetaxi.org/chapters-and-colonies/")
    stubs = DirectoryV1Adapter().parse_stubs(fixture, "https://www.thetaxi.org/chapters-and-colonies/")

    assert len(records) == 5
    assert records[0].name == "Beta Zeta"
    assert records[0].university_name == "Auburn University"
    assert len(stubs) == 5
    assert stubs[0].provenance == "directory_v1:repeated_list"


def test_directory_adapter_extracts_mixed_structured_chapter_cards():
    fixture = """
    <section class="chapters-grid">
      <div class="col-xs-12 col-sm-6 col-md-4 col-lg-3 chapter norwich-university alpha">
        <div class="chapter-item">
          <div class="chapter-logo"></div>
          <h2>Alpha</h2>
          <h3>Norwich University</h3>
        </div>
      </div>
      <div class="col-xs-12 col-sm-6 col-md-4 col-lg-3 chapter university-of-rhode-island eta">
        <a class="chapter-link" href="/eta">
          <div class="chapter-logo"></div>
          <h2>Eta</h2>
          <h3>University of Rhode Island</h3>
        </a>
      </div>
    </section>
    """

    records = DirectoryV1Adapter().parse(fixture, "https://www.thetachi.org/chapters")
    stubs = DirectoryV1Adapter().parse_stubs(fixture, "https://www.thetachi.org/chapters")

    assert len(records) == 2
    assert {record.name for record in records} == {"Alpha", "Eta"}
    eta_record = next(record for record in records if record.name == "Eta")
    assert eta_record.university_name == "University of Rhode Island"
    assert eta_record.website_url == "https://www.thetachi.org/eta"

    assert len(stubs) == 2
    alpha_stub = next(stub for stub in stubs if stub.chapter_name == "Alpha")
    eta_stub = next(stub for stub in stubs if stub.chapter_name == "Eta")
    assert alpha_stub.university_name == "Norwich University"
    assert alpha_stub.detail_url == "https://www.thetachi.org/chapters"
    assert eta_stub.outbound_chapter_url_candidate == "https://www.thetachi.org/eta"
