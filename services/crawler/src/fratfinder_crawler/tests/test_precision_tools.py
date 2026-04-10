from fratfinder_crawler.precision_tools import (
    tool_campus_greek_life_policy,
    tool_directory_block_matcher,
    tool_directory_layout_profiler,
    tool_greek_detection,
    tool_official_domain_verifier,
    tool_school_chapter_list_validator,
    tool_same_host_directory_ranker,
    tool_site_scope_classifier,
    tool_source_identity_guard,
)


def test_source_identity_guard_rejects_cross_fraternity_candidate():
    decision = tool_source_identity_guard(
        fraternity_name="Theta Chi",
        fraternity_slug="theta-chi",
        candidate_url="https://www.kkpsi.org/about/chapters-districts/chapter-listing-2/",
        title="Kappa Kappa Psi Chapter Listing",
        snippet="Official Kappa Kappa Psi chapters and districts listing.",
    )

    assert decision.decision == "reject"
    assert "cross_fraternity_conflict" in decision.reason_codes


def test_same_host_directory_ranker_prefers_chapters_over_staff_directory():
    html = """
    <html><body>
      <nav>
        <a href="/staff-directory">Staff Directory</a>
        <a href="/chapters">Chapters</a>
      </nav>
    </body></html>
    """

    decision = tool_same_host_directory_ranker(
        source_url="https://www.thetachi.org/about",
        html=html,
    )

    assert decision.decision == "ranked_directory_link"
    assert decision.metadata["selectedUrl"] == "https://www.thetachi.org/chapters"


def test_directory_layout_profiler_detects_mixed_card_grid():
    html = """
    <html><body>
      <section class="chapters-grid">
        <div class="chapter-item">
          <h2>Alpha Beta</h2>
          <h3>State University</h3>
        </div>
        <a class="chapter-link" href="/eta">
          <h2>Eta</h2>
          <h3>University of Rhode Island</h3>
        </a>
      </section>
    </body></html>
    """

    decision = tool_directory_layout_profiler(
        html=html,
        page_url="https://www.thetachi.org/chapters",
    )

    assert decision.decision == "directory_layout_profiled"
    assert decision.metadata["layoutFamily"] == "mixed_card_grid"
    assert decision.metadata["recommendedStrategy"] == "repeated_block"


def test_official_domain_verifier_rejects_cross_fraternity_candidate():
    decision = tool_official_domain_verifier(
        candidate_url="https://www.kkpsi.org/eta",
        fraternity_name="Theta Chi",
        fraternity_slug="theta-chi",
        chapter_name="Eta Chapter",
        university_name="University of Rhode Island",
        source_url="https://www.thetachi.org/chapters",
        document_url="https://www.kkpsi.org/about/chapters-districts/chapter-listing-2/",
        document_title="Kappa Kappa Psi Chapter Listing",
        document_text="Kappa Kappa Psi official district and chapter directory for University of Rhode Island.",
    )

    assert decision.decision == "reject"
    assert "cross_fraternity_conflict" in decision.reason_codes


def test_official_domain_verifier_accepts_official_school_affiliation_page():
    decision = tool_official_domain_verifier(
        candidate_url="https://fsl.uri.edu/theta-chi",
        fraternity_name="Theta Chi",
        fraternity_slug="theta-chi",
        chapter_name="Eta Chapter",
        university_name="University of Rhode Island",
        source_url="https://www.thetachi.org/chapters",
        document_url="https://fsl.uri.edu/theta-chi",
        document_title="Theta Chi | University of Rhode Island Greek Life",
        document_text="Recognized fraternity chapter profile for Theta Chi at the University of Rhode Island.",
    )

    assert decision.decision in {"official_affiliation_page", "weak_match"}
    assert decision.confidence >= 0.54


def test_official_domain_verifier_rejects_map_export_url():
    decision = tool_official_domain_verifier(
        candidate_url="https://www.google.com/maps/d/kml?mid=1497z-lFQzqOBrDnwB3z0r_qiqNU&forcekml=1",
        fraternity_name="Phi Gamma Delta",
        fraternity_slug="phi-gamma-delta",
        chapter_name="Beta Rho Chapter",
        university_name="Louisiana State University",
        source_url="https://phigam.org/about/overview/our-chapters/",
        document_url="https://phigam.org/about/overview/our-chapters/",
        document_title="Our Chapters",
        document_text="Chapter map and chapter directory.",
    )

    assert decision.decision == "reject"
    assert "map_export_url" in decision.reason_codes


def test_official_domain_verifier_rejects_wrong_school_affiliation_page():
    decision = tool_official_domain_verifier(
        candidate_url="https://drexel.edu/studentlife/activities-involvement/fraternity-sorority-life/councils-and-chapters/fraternities",
        fraternity_name="Theta Chi",
        fraternity_slug="theta-chi",
        chapter_name="Kappa Chapter",
        university_name="University of Pennsylvania",
        source_url="https://www.thetachi.org/chapters",
        document_url="https://drexel.edu/studentlife/activities-involvement/fraternity-sorority-life/councils-and-chapters/fraternities",
        document_title="Fraternities | Drexel University",
        document_text="Drexel University fraternity and sorority life chapter roster including Theta Chi.",
    )

    assert decision.decision == "reject"
    assert any(reason in decision.reason_codes for reason in {"missing_target_school_context", "generic_school_directory"})


def test_official_domain_verifier_rejects_wrong_school_dot_edu_even_with_supporting_context():
    decision = tool_official_domain_verifier(
        candidate_url="https://www.rider.edu/about/offices-services/student-involvement/fraternities-sororities/recognized/theta-chi",
        fraternity_name="Theta Chi",
        fraternity_slug="theta-chi",
        chapter_name="Gamma Omega",
        university_name="Vanderbilt University",
        source_url="https://www.instagram.com/vandythetachi/",
        document_url="https://www.instagram.com/vandythetachi/",
        document_title="Theta Chi Vanderbilt (@vandythetachi)",
        document_text="Theta Chi Vanderbilt social profile and recruitment updates.",
    )

    assert decision.decision == "reject"
    assert "missing_target_school_context" in decision.reason_codes


def test_official_domain_verifier_accepts_school_chapter_profile_page():
    decision = tool_official_domain_verifier(
        candidate_url="https://www.uwyo.edu/fsl/aboutus/chapter-page/sigma-chi.html",
        fraternity_name="Sigma Chi",
        fraternity_slug="sigma-chi",
        chapter_name="Gamma Xi",
        university_name="University of Wyoming",
        source_url="https://www.sigmachi.org/chapters/",
        document_url="https://www.uwyo.edu/fsl/aboutus/chapter-page/sigma-chi.html",
        document_title="Sigma Chi (Gamma Xi Chapter)",
        document_text="University of Wyoming Fraternity and Sorority Life chapter profile for Sigma Chi.",
        document_html='<title>Sigma Chi (Gamma Xi Chapter)</title><div>University of Wyoming</div><div>Fraternity and Sorority Life</div>',
    )

    assert decision.decision == "official_affiliation_page"
    assert "chapter_specific_school_page" in decision.reason_codes


def test_official_domain_verifier_accepts_school_org_portal_when_html_contains_identity():
    decision = tool_official_domain_verifier(
        candidate_url="https://terplink.umd.edu/organization/alpha-gamma-rho",
        fraternity_name="Alpha Gamma Rho",
        fraternity_slug="alpha-gamma-rho",
        chapter_name="Alpha Theta",
        university_name="University of Maryland-College Park",
        source_url="https://alphagammarho.org/chapters",
        document_url="https://terplink.umd.edu/organization/alpha-gamma-rho",
        document_title=" - TerpLink",
        document_text="Discover unique opportunities at TerpLink.",
        document_html='{"organization":{"name":"Alpha Gamma Rho","websitekey":"alpha-gamma-rho","description":"Established in 1928, Alpha Gamma Rho is the premier agricultural fraternity at the University of Maryland.","email":"alphathetaumd@gmail.com"}}',
    )

    assert decision.decision == "official_affiliation_page"
    assert "chapter_specific_school_page" in decision.reason_codes


def test_official_domain_verifier_rejects_generic_school_directory_page():
    decision = tool_official_domain_verifier(
        candidate_url="https://studentaffairs.psu.edu/student-life/fraternity-sorority-life/councils-chapters/ifc",
        fraternity_name="Alpha Tau Omega",
        fraternity_slug="alpha-tau-omega",
        chapter_name="Gamma Omega",
        university_name="Penn State",
        source_url="https://ato.org/chapters/",
        document_url="https://studentaffairs.psu.edu/student-life/fraternity-sorority-life/councils-chapters/ifc",
        document_title="Interfraternity Council | Penn State Student Affairs",
        document_text="Interfraternity Council (IFC) is the governing body for fraternity chapters at Penn State.",
        document_html="<title>Interfraternity Council | Penn State Student Affairs</title>",
    )

    assert decision.decision == "reject"
    assert "generic_school_directory" in decision.reason_codes


def test_official_domain_verifier_rejects_school_profile_without_target_fraternity_context():
    decision = tool_official_domain_verifier(
        candidate_url="https://law.utexas.edu/barristers/chapters-and-members/southern-methodist-university/",
        fraternity_name="Sigma Chi",
        fraternity_slug="sigma-chi",
        chapter_name="Delta Mu",
        university_name="Southern Methodist University",
        source_url="https://sigmachi.org/chapters/",
        document_url="https://law.utexas.edu/barristers/chapters-and-members/southern-methodist-university/",
        document_title="Southern Methodist University Dedman School of Law | The Order of Barristers | Texas Law",
        document_text="Southern Methodist University Dedman School of Law chapter page for the Order of Barristers at Texas Law.",
        document_html="<title>Southern Methodist University Dedman School of Law | The Order of Barristers | Texas Law</title>",
    )

    assert decision.decision == "reject"


def test_official_domain_verifier_rejects_generic_school_org_portal_root():
    decision = tool_official_domain_verifier(
        candidate_url="https://indstate.campuslabs.com/engage",
        fraternity_name="Lambda Chi Alpha",
        fraternity_slug="lambda-chi-alpha",
        chapter_name="Iota-Epsilon",
        university_name="Indiana State University",
        source_url="https://www.lambdachi.org/chapters/iota-epsilon-indiana-state/",
        document_url="https://indstate.campuslabs.com/engage",
        document_title="Treehouse",
        document_text="Discover unique opportunities at Treehouse! Find and attend events, browse and join organizations, and showcase your involvement.",
        document_html='<meta name="description" content="Discover unique opportunities at Treehouse! Find and attend events, browse and join organizations, and showcase your involvement.">',
    )

    assert decision.decision == "reject"
    assert "generic_school_directory" in decision.reason_codes


def test_official_domain_verifier_rejects_missing_school_page():
    decision = tool_official_domain_verifier(
        candidate_url="https://sites.udel.edu/agr/",
        fraternity_name="Alpha Gamma Rho",
        fraternity_slug="alpha-gamma-rho",
        chapter_name="Beta Upsilon",
        university_name="University of Delaware",
        source_url="https://alphagammarho.org/chapters",
        document_url="https://sites.udel.edu/agr/",
        document_title="WordPress Error",
        document_text="This site is no longer available.",
        document_html="<title>WordPress Error</title><div>This site is no longer available.</div>",
    )

    assert decision.decision == "reject"
    assert "page_missing" in decision.reason_codes


def test_campus_greek_life_policy_requires_official_school_source_for_ban():
    decision = tool_campus_greek_life_policy(
        school_name="Norwich University",
        page_url="https://www.thetachi.org/alpha-chapter-closed-56-years-ago-today",
        title="Alpha Chapter Closed 56 Years Ago Today",
        text="There are no fraternities at Norwich. They were banned from campus in 1959.",
    )

    assert decision.decision == "unknown"
    assert "non_official_school_source" in decision.reason_codes


def test_campus_greek_life_policy_accepts_official_school_ban_signal():
    decision = tool_campus_greek_life_policy(
        school_name="Norwich University",
        page_url="https://archives.norwich.edu/fraternities-banned",
        title="Norwich fraternities are no longer fraternities by any definition",
        text="Norwich fraternities are no longer fraternities by any definition and there are no fraternities on campus.",
    )

    assert decision.decision == "banned"
    assert decision.confidence >= 0.9


def test_campus_greek_life_policy_keeps_weak_official_context_unknown():
    decision = tool_campus_greek_life_policy(
        school_name="Cornell University",
        page_url="https://hazing.cornell.edu/violations",
        title="Violations | Cornell Hazing",
        text="Violations and conduct resources for students, including fraternity and sorority life references.",
    )

    assert decision.decision == "unknown"
    assert "no_conclusive_policy_signal" in decision.reason_codes


def test_campus_greek_life_policy_rejects_unrelated_dot_edu_article():
    decision = tool_campus_greek_life_policy(
        school_name="Troy University",
        page_url="https://ceneval.unicah.edu/us/the-shocking-truth-about-greek-life-hazing-at-university-of-alabama",
        title="The shocking truth about greek life hazing at University of Alabama",
        text="Greek life policies and hazing concerns at several campuses, including references to Troy University.",
    )

    assert decision.decision == "unknown"
    assert "non_official_school_source" in decision.reason_codes


def test_school_chapter_list_validator_marks_absent_fraternity_inactive():
    html = """
    <html><body>
      <h1>Chapters at Penn</h1>
      <a href="/alpha-chi-rho">Alpha Chi Rho</a>
      <a href="/phi-gamma-delta">Phi Gamma Delta</a>
      <a href="/sigma-chi">Sigma Chi</a>
    </body></html>
    """

    decision = tool_school_chapter_list_validator(
        school_name="University of Pennsylvania",
        fraternity_name="Theta Chi",
        fraternity_slug="theta-chi",
        page_url="https://ofsl.universitylife.upenn.edu/chapters/",
        title="Chapters at Penn",
        text="Recognized chapters and fraternities at Penn.",
        html=html,
    )

    assert decision.decision == "confirmed_inactive"


def test_school_chapter_list_validator_keeps_generic_community_page_unknown():
    html = """
    <html><body>
      <h1>Our Community</h1>
      <a href="/greeks/councils">Councils and Chapters</a>
      <a href="/greeks/scorecard/index.php">Community Scorecard</a>
      <a href="/ifc">Interfraternity Council</a>
    </body></html>
    """

    decision = tool_school_chapter_list_validator(
        school_name="Louisiana State University",
        fraternity_name="Phi Gamma Delta",
        fraternity_slug="phi-gamma-delta",
        page_url="https://www.lsu.edu/greeks/community/index.php",
        title="Our Community",
        text="Interfraternity Council and Greek community resources.",
        html=html,
    )

    assert decision.decision == "unknown"
    assert "official_page_not_conclusive" in decision.reason_codes


def test_school_chapter_list_validator_accepts_tabbed_scorecard_page():
    html = """
    <html><body>
      <h1>Community Scorecard</h1>
      <p>Active Chapters</p>
      <a href="#fraternities">Fraternities</a>
      <a href="#sororities">Sororities</a>
      <a href="#suspended">Suspended Chapters</a>
      <a href="#closed">Closed Chapters</a>
      <h3>Phi Gamma Delta</h3>
      <p>Active</p>
      <p>FIJI</p>
      <a href="/greeks/scorecard/fiji">View Scorecard</a>
      <h3>Sigma Chi</h3>
      <p>Active</p>
      <a href="/greeks/scorecard/sigma-chi">View Scorecard</a>
      <h3>Delta Chi</h3>
      <p>Active</p>
      <a href="/greeks/scorecard/delta-chi">View Scorecard</a>
    </body></html>
    """

    decision = tool_school_chapter_list_validator(
        school_name="Louisiana State University",
        fraternity_name="Phi Gamma Delta",
        fraternity_slug="phi-gamma-delta",
        page_url="https://www.lsu.edu/greeks/scorecard/index.php",
        title="Community Scorecard | Greek Life",
        text="Active Chapters Chapter Scorecards Fraternities Sororities Suspended Chapters Closed Chapters Phi Gamma Delta Active FIJI.",
        html=html,
    )

    assert decision.decision == "confirmed_active"


def test_school_chapter_list_validator_uses_html_roster_when_text_is_truncated():
    html = """
    <html><body>
      <h1>Community Scorecard</h1>
      <div class="tabs">
        <nav>
          <a href="#frat">Fraternities</a>
          <a href="#sor">Sororities</a>
          <a href="#sus">Suspended Chapters</a>
          <a href="#closed">Closed Chapters</a>
        </nav>
        <div id="frat" class="tab-pane fade show active">
          <div><h3>Acacia</h3><p>Active</p><a href="/greeks/scorecard/acacia">View Scorecard</a></div>
          <div><h3>Phi Gamma Delta</h3><p>Active</p><p>FIJI</p><a href="/greeks/scorecard/fiji">View Scorecard</a></div>
          <div><h3>Sigma Chi</h3><p>Active</p><a href="/greeks/scorecard/sigma-chi">View Scorecard</a></div>
        </div>
      </div>
    </body></html>
    """

    decision = tool_school_chapter_list_validator(
        school_name="Louisiana State University",
        fraternity_name="Phi Gamma Delta",
        fraternity_slug="phi-gamma-delta",
        page_url="https://www.lsu.edu/greeks/scorecard/index.php",
        title="Community Scorecard | Greek Life",
        text="Community Scorecard Fraternities Sororities Suspended Chapters Closed Chapters",
        html=html,
    )

    assert decision.decision == "confirmed_active"


def test_school_chapter_list_validator_ignores_historical_archive_page():
    html = """
    <html><body>
      <h1>Historical fraternities</h1>
      <ul>
        <li>Theta Chi (Alpha Chapter)</li>
        <li>Lambda Chi Alpha</li>
        <li>Sigma Nu</li>
      </ul>
    </body></html>
    """

    decision = tool_school_chapter_list_validator(
        school_name="Norwich University",
        fraternity_name="Theta Chi",
        fraternity_slug="theta-chi",
        page_url="https://archives.norwich.edu/fraternities-history",
        title="Historical fraternities at Norwich",
        text="Archive of former fraternity and sorority organizations at Norwich University.",
        html=html,
    )

    assert decision.decision == "unknown"
    assert "historical_school_context" in decision.reason_codes


def test_school_chapter_list_validator_rejects_official_school_news_article_as_roster():
    html = """
    <html><body>
      <article>
        <h1>Department spotlight: Center for Fraternity and Sorority Life</h1>
        <p>Washington State University supports fraternity and sorority life.</p>
        <p>Our chapters focus on scholarship, leadership, and service.</p>
        <p>Theta Chi and other fraternities contribute to the community.</p>
      </article>
    </body></html>
    """

    decision = tool_school_chapter_list_validator(
        school_name="Washington State University",
        fraternity_name="Theta Chi",
        fraternity_slug="theta-chi",
        page_url="https://news.wsu.edu/announcements/department-spotlight-center-fraternity-sorority-life-cfsl/",
        title="Department spotlight: Center for Fraternity and Sorority Life",
        text="Washington State University fraternity and sorority life spotlight featuring student support and campus leadership programs.",
        html=html,
    )

    assert decision.decision == "unknown"
    assert "school_article_context" in decision.reason_codes


def test_directory_block_matcher_finds_school_block_with_greek_tokens():
    html = """
    <div class="wpgmp_iw_content">
      <div>
        University of Pennsylvania Phi Phi <br/>
        Philadelphia, PA <br/>
        <a href="http://phiphiclub.com/" target="_blank">Chapter Website</a>
      </div>
    </div>
    """

    decision = tool_directory_block_matcher(
        html=html,
        page_url="https://www.alphachirho.org/about/map/",
        school_name="University of Pennsylvania",
        fraternity_name="Alpha Chi Rho",
        chapter_name="Phi Phi",
    )

    assert decision.decision == "matched_block"
    assert "http://phiphiclub.com/" in decision.metadata["links"]


def test_greek_detection_finds_tokens_in_mixed_school_string():
    decision = tool_greek_detection(value="University of Pennsylvania Phi Phi")

    assert decision.decision == "greek_tokens_found"
    assert set(decision.metadata["tokens"]) >= {"phi"}


def test_site_scope_classifier_marks_school_affiliation_pages():
    decision = tool_site_scope_classifier(
        page_url="https://ofsl.universitylife.upenn.edu/chapters/",
        title="Chapters at Penn",
        text="Recognized chapters, greek life, and fraternities at Penn.",
        fraternity_name="Theta Chi",
        school_name="University of Pennsylvania",
        chapter_name="Kappa",
    )

    assert decision.decision == "school_affiliation"


def test_site_scope_classifier_keeps_standalone_fraternity_host_out_of_school_affiliation():
    decision = tool_site_scope_classifier(
        page_url="https://sdstateagr.com/contact",
        title="Contact | SD State Agr",
        text="Alpha Gamma Rho at South Dakota State University contact information and chapter house details.",
        fraternity_name="Alpha Gamma Rho",
        school_name="South Dakota State University",
        chapter_name="Alpha Phi",
    )

    assert decision.decision == "chapter_site"


def test_site_scope_classifier_does_not_treat_two_letter_fraternity_initials_as_identity():
    decision = tool_site_scope_classifier(
        page_url="https://www.uc.edu/about/digital-accessibility/contact-support/report-concern.html",
        title="Report eAccessibility Concern - University of Cincinnati",
        text="Use the form to report accessibility concerns to the University of Cincinnati digital accessibility team.",
        fraternity_name="Sigma Chi",
        school_name="University of Cincinnati",
        chapter_name="Zeta Psi",
    )

    assert decision.decision != "chapter_site"
