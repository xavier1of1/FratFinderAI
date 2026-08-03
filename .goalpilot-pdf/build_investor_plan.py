from __future__ import annotations

from pathlib import Path
from math import ceil

from reportlab.lib import colors
from reportlab.lib.colors import HexColor
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import inch
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfgen import canvas
from reportlab.platypus import Paragraph, Table, TableStyle
from pypdf import PdfReader

OUT_DIR = Path(__file__).resolve().parent / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
PDF_PATH = OUT_DIR / "GoalPilot_Cash_Investor_Business_Plan.pdf"
TEXT_PATH = OUT_DIR / "GoalPilot_Cash_Investor_Business_Plan_validation.txt"

W, H = LETTER
M = 48
CONTENT_W = W - 2 * M

BRG = HexColor("#003B2A")
BRG_2 = HexColor("#07533A")
BRG_3 = HexColor("#1E6A4E")
MINT = HexColor("#DCEBE4")
PALE = HexColor("#F3F7F4")
CREAM = HexColor("#F5F1E7")
GOLD = HexColor("#B89A5A")
INK = HexColor("#17231D")
SLATE = HexColor("#526158")
MID = HexColor("#8A958E")
LINE = HexColor("#D6DED9")
WHITE = colors.white
RED = HexColor("#9A3F3F")
AMBER = HexColor("#A66D20")
BLUE = HexColor("#315A73")

TITLE = "GoalPilot Cash"
SUBTITLE = "Investor Business Plan"
DATE_LABEL = "August 2026"
CONFIDENTIAL = "CONFIDENTIAL • INVESTOR DISCUSSION DRAFT"
DISCLAIMER = (
    "This document is a planning artifact, not an offer of securities, a bank account, or financial advice. "
    "Provider availability, pricing, regulatory allocation, and deposit insurance treatment require commercial and legal diligence."
)

styles = {
    "cover_kicker": ParagraphStyle(
        "cover_kicker", fontName="Helvetica-Bold", fontSize=10, leading=13,
        textColor=GOLD, tracking=1.6, spaceAfter=0
    ),
    "cover_title": ParagraphStyle(
        "cover_title", fontName="Helvetica-Bold", fontSize=35, leading=38,
        textColor=WHITE, spaceAfter=0
    ),
    "cover_subtitle": ParagraphStyle(
        "cover_subtitle", fontName="Helvetica", fontSize=18, leading=23,
        textColor=HexColor("#DDE8E1"), spaceAfter=0
    ),
    "section_num": ParagraphStyle(
        "section_num", fontName="Helvetica-Bold", fontSize=9, leading=11,
        textColor=GOLD, tracking=1.2
    ),
    "h1": ParagraphStyle(
        "h1", fontName="Helvetica-Bold", fontSize=23, leading=27,
        textColor=INK, spaceAfter=0
    ),
    "h2": ParagraphStyle(
        "h2", fontName="Helvetica-Bold", fontSize=14, leading=17,
        textColor=BRG, spaceBefore=0, spaceAfter=0
    ),
    "h3": ParagraphStyle(
        "h3", fontName="Helvetica-Bold", fontSize=10.5, leading=13,
        textColor=INK, spaceAfter=0
    ),
    "body": ParagraphStyle(
        "body", fontName="Helvetica", fontSize=9.3, leading=13.2,
        textColor=INK, spaceAfter=0
    ),
    "body_small": ParagraphStyle(
        "body_small", fontName="Helvetica", fontSize=7.9, leading=10.7,
        textColor=INK, spaceAfter=0
    ),
    "body_muted": ParagraphStyle(
        "body_muted", fontName="Helvetica", fontSize=8.5, leading=11.6,
        textColor=SLATE, spaceAfter=0
    ),
    "callout": ParagraphStyle(
        "callout", fontName="Helvetica-Bold", fontSize=12, leading=16,
        textColor=BRG, spaceAfter=0
    ),
    "metric": ParagraphStyle(
        "metric", fontName="Helvetica-Bold", fontSize=23, leading=24,
        textColor=BRG, alignment=TA_CENTER
    ),
    "metric_label": ParagraphStyle(
        "metric_label", fontName="Helvetica", fontSize=7.8, leading=10.2,
        textColor=SLATE, alignment=TA_CENTER
    ),
    "table_head": ParagraphStyle(
        "table_head", fontName="Helvetica-Bold", fontSize=7.5, leading=9.2,
        textColor=WHITE
    ),
    "table_cell": ParagraphStyle(
        "table_cell", fontName="Helvetica", fontSize=7.3, leading=9.6,
        textColor=INK
    ),
    "table_cell_bold": ParagraphStyle(
        "table_cell_bold", fontName="Helvetica-Bold", fontSize=7.3, leading=9.6,
        textColor=INK
    ),
    "footnote": ParagraphStyle(
        "footnote", fontName="Helvetica", fontSize=6.5, leading=8.3,
        textColor=SLATE
    ),
    "source": ParagraphStyle(
        "source", fontName="Helvetica", fontSize=6.4, leading=8.4,
        textColor=INK
    ),
    "source_num": ParagraphStyle(
        "source_num", fontName="Helvetica-Bold", fontSize=7, leading=8.4,
        textColor=BRG
    ),
}


def para(c: canvas.Canvas, text: str, style: ParagraphStyle, x: float, y_top: float, width: float, max_h: float = 1000) -> float:
    p = Paragraph(text, style)
    w, h = p.wrap(width, max_h)
    p.drawOn(c, x, y_top - h)
    return y_top - h


def table(c: canvas.Canvas, data, col_widths, x, y_top, style_cmds, row_heights=None, max_h=1000):
    t = Table(data, colWidths=col_widths, rowHeights=row_heights, hAlign="LEFT")
    t.setStyle(TableStyle(style_cmds))
    w, h = t.wrap(sum(col_widths), max_h)
    t.drawOn(c, x, y_top - h)
    return y_top - h


def P(text, style="table_cell"):
    return Paragraph(text, styles[style])


def round_rect(c, x, y, w, h, fill, stroke=None, radius=8, line_width=1):
    c.setLineWidth(line_width)
    c.setFillColor(fill)
    c.setStrokeColor(stroke or fill)
    c.roundRect(x, y, w, h, radius, fill=1, stroke=1 if stroke else 0)


def logo_mark(c, x, y, size=30, dark=False):
    fg = WHITE if dark else BRG
    accent = GOLD
    c.saveState()
    c.setStrokeColor(fg)
    c.setLineWidth(max(1.2, size * 0.055))
    c.circle(x + size / 2, y + size / 2, size * 0.42, stroke=1, fill=0)
    c.circle(x + size / 2, y + size / 2, size * 0.22, stroke=1, fill=0)
    c.setStrokeColor(accent)
    c.setLineWidth(max(1.4, size * 0.065))
    c.line(x + size * 0.50, y + size * 0.26, x + size * 0.69, y + size * 0.72)
    c.line(x + size * 0.69, y + size * 0.72, x + size * 0.58, y + size * 0.65)
    c.line(x + size * 0.69, y + size * 0.72, x + size * 0.70, y + size * 0.59)
    c.restoreState()


def header(c, section):
    logo_mark(c, M, H - 35, 22, dark=False)
    c.setFillColor(BRG)
    c.setFont("Helvetica-Bold", 9)
    c.drawString(M + 29, H - 27, "GOALPILOT")
    c.setFillColor(MID)
    c.setFont("Helvetica", 7.5)
    c.drawRightString(W - M, H - 27, section.upper())
    c.setStrokeColor(LINE)
    c.setLineWidth(0.6)
    c.line(M, H - 42, W - M, H - 42)


def footer(c, page_no):
    c.setStrokeColor(LINE)
    c.setLineWidth(0.5)
    c.line(M, 34, W - M, 34)
    c.setFillColor(MID)
    c.setFont("Helvetica", 6.5)
    c.drawString(M, 22, CONFIDENTIAL)
    c.drawRightString(W - M, 22, f"GOALPILOT CASH • {page_no:02d}")


def page_title(c, number, title, subtitle=None):
    y = H - 65
    y = para(c, number.upper(), styles["section_num"], M, y, CONTENT_W)
    y -= 4
    y = para(c, title, styles["h1"], M, y, CONTENT_W)
    if subtitle:
        y -= 5
        y = para(c, subtitle, styles["body_muted"], M, y, CONTENT_W)
    return y - 13


def bullet_list(c, items, x, y, width, font_size=8.7, leading=12, bullet_color=GOLD, gap=4):
    style = ParagraphStyle(
        "bullet_dynamic", parent=styles["body"], fontSize=font_size, leading=leading,
        leftIndent=12, firstLineIndent=-10, bulletIndent=0, textColor=INK
    )
    for item in items:
        c.setFillColor(bullet_color)
        c.circle(x + 2.5, y - 5.3, 1.8, fill=1, stroke=0)
        y = para(c, item, style, x + 10, y, width - 10)
        y -= gap
    return y


def metric_card(c, x, y, w, h, value, label, note=None):
    round_rect(c, x, y, w, h, PALE, LINE, 8, 0.7)
    para(c, value, styles["metric"], x + 8, y + h - 14, w - 16)
    para(c, label, styles["metric_label"], x + 9, y + h - 45, w - 18)
    if note:
        c.setFillColor(MID)
        c.setFont("Helvetica", 5.8)
        c.drawCentredString(x + w / 2, y + 8, note)


def flow_step(c, x, y, w, h, n, title, detail):
    round_rect(c, x, y, w, h, WHITE, LINE, 8, 0.7)
    c.setFillColor(BRG)
    c.circle(x + 15, y + h - 16, 8.5, fill=1, stroke=0)
    c.setFillColor(WHITE)
    c.setFont("Helvetica-Bold", 7.5)
    c.drawCentredString(x + 15, y + h - 18.5, str(n))
    para(c, title, styles["h3"], x + 29, y + h - 10, w - 36)
    para(c, detail, styles["body_small"], x + 10, y + h - 34, w - 20)


def draw_cover(c):
    c.setFillColor(BRG)
    c.rect(0, 0, W, H, fill=1, stroke=0)
    # Large subtle target motif.
    c.saveState()
    c.setStrokeColor(HexColor("#175340"))
    c.setLineWidth(1)
    cx, cy = W - 72, H - 90
    for r in (54, 86, 118, 150):
        c.circle(cx, cy, r, stroke=1, fill=0)
    c.restoreState()

    logo_mark(c, M, H - 92, 42, dark=True)
    c.setFillColor(WHITE)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(M + 54, H - 68, "GOALPILOT")
    c.setFillColor(GOLD)
    c.setFont("Helvetica-Bold", 8)
    c.drawString(M + 54, H - 82, "GOAL-BASED FINANCIAL AUTOMATION")

    y = H - 174
    y = para(c, "INVESTOR BUSINESS PLAN", styles["cover_kicker"], M, y, 365)
    y -= 14
    y = para(c, "GoalPilot Cash", styles["cover_title"], M, y, 430)
    y -= 8
    y = para(c, "An automated, interest-earning account for planned purchases", styles["cover_subtitle"], M, y, 430)

    # Thesis box.
    box_y = 270
    round_rect(c, M, box_y, 405, 166, HexColor("#0A4A37"), HexColor("#2E6856"), 12, 0.7)
    para(c, "THE INVESTMENT THESIS", styles["cover_kicker"], M + 20, box_y + 143, 360)
    thesis_style = ParagraphStyle(
        "cover_thesis", fontName="Helvetica", fontSize=10.5, leading=15,
        textColor=WHITE, leftIndent=15, firstLineIndent=-13
    )
    items = [
        "<b>Clear consumer job:</b> turn a desired purchase into a funded plan, without debt.",
        "<b>Aligned economics:</b> membership, deposit-program revenue, and purchase-completion revenue grow when the user saves successfully.",
        "<b>Focused launch:</b> one user, one goal, one customer-owned interest-bearing account, and one recurring installment plan."
    ]
    yy = box_y + 118
    for item in items:
        c.setFillColor(GOLD)
        c.circle(M + 24, yy - 6, 2, fill=1, stroke=0)
        yy = para(c, item, thesis_style, M + 31, yy, 350)
        yy -= 10

    c.setFillColor(HexColor("#C9D9D1"))
    c.setFont("Helvetica", 9)
    c.drawString(M, 92, DATE_LABEL)
    c.setFont("Helvetica-Bold", 7)
    c.drawString(M, 73, CONFIDENTIAL)
    c.setFont("Helvetica", 6.5)
    c.drawString(M, 57, "Prepared for investor and strategic-partner discussion")

    c.setFillColor(GOLD)
    c.rect(W - 16, 0, 16, H, fill=1, stroke=0)
    c.showPage()


def draw_page_2(c):
    header(c, "Executive summary")
    y = page_title(
        c, "01", "A purpose-built account for planned purchases",
        "GoalPilot Cash converts intention into an automated funding plan, then keeps the money productive until the user is ready to buy."
    )

    # Executive summary narrative.
    left_w = 320
    y_left = y
    y_left = para(c,
        "Consumers can earmark money at a bank, automate transfers in a budgeting app, or finance a purchase after the fact. "
        "Few products combine the <b>purchase target, deadline, recurring installment, interest-bearing account, and completion workflow</b> in one experience.",
        styles["body"], M, y_left, left_w)
    y_left -= 12
    y_left = para(c,
        "GoalPilot Cash is designed as a positive alternative to credit-led consumption. The user chooses a goal, authorizes recurring ACH installments, "
        "and receives a customer-owned deposit account through a regulated partner. GoalPilot manages the plan and the experience; the partner bank and embedded-finance provider manage custody, account issuance, interest, statements, and applicable banking operations.",
        styles["body"], M, y_left, left_w)

    # Core proposition card.
    card_x = M + 338
    card_y = y - 158
    round_rect(c, card_x, card_y, 174, 162, BRG, BRG, 10)
    para(c, "CORE PROMISE", styles["cover_kicker"], card_x + 16, card_y + 142, 140)
    quote_style = ParagraphStyle(
        "quote", fontName="Helvetica-Bold", fontSize=14.5, leading=19,
        textColor=WHITE
    )
    para(c, "Earn interest instead of paying interest.", quote_style, card_x + 16, card_y + 111, 142)
    para(c,
        "Choose a purchase. Set the amount and deadline. Automate the installments. Withdraw when the goal is funded.",
        ParagraphStyle("qsmall", fontName="Helvetica", fontSize=8.6, leading=12, textColor=HexColor("#DDE8E1")),
        card_x + 16, card_y + 54, 142)

    y2 = min(y_left, card_y - 18)
    para(c, "WHY THIS WEDGE IS CREDIBLE", styles["h2"], M, y2, CONTENT_W)
    y2 -= 23
    gap = 10
    cw = (CONTENT_W - 2 * gap) / 3
    metric_card(c, M, y2 - 100, cw, 100, "96%", "of U.S. households were banked in 2023<super>1</super>", "FDIC household survey")
    metric_card(c, M + cw + gap, y2 - 100, cw, 100, "48.3%", "of banked households used mobile banking as their primary access method<super>1</super>", "FDIC household survey")
    metric_card(c, M + 2 * (cw + gap), y2 - 100, cw, 100, "63%", "of adults could cover a $400 emergency with cash or its equivalent in 2025<super>2</super>", "Federal Reserve SHED")

    y3 = y2 - 124
    para(c, "The initial beta", styles["h2"], M, y3, CONTENT_W)
    y3 -= 20
    beta_data = [
        [P("Product boundary", "table_head"), P("Beta design", "table_head"), P("Why it matters", "table_head")],
        [P("Account"), P("One individually titled partner-bank account"), P("Clear ownership and simpler reconciliation")],
        [P("Goal"), P("One active purchase goal"), P("Focused user learning and lower operational complexity")],
        [P("Funding"), P("One verified external bank; standard ACH"), P("Controlled transfers and a narrow error surface")],
        [P("Asset"), P("Interest-bearing deposit only"), P("No market loss, trading, or investment-advice boundary")],
        [P("Users"), P("Maximum 50 invited U.S. adults"), P("High-touch support and direct operational observation")],
    ]
    y3 = table(c, beta_data, [105, 188, 219], M, y3,
        [("BACKGROUND", (0,0), (-1,0), BRG), ("TEXTCOLOR", (0,0), (-1,0), WHITE),
         ("GRID", (0,0), (-1,-1), 0.35, LINE), ("VALIGN", (0,0), (-1,-1), "TOP"),
         ("LEFTPADDING", (0,0), (-1,-1), 7), ("RIGHTPADDING", (0,0), (-1,-1), 7),
         ("TOPPADDING", (0,0), (-1,-1), 6), ("BOTTOMPADDING", (0,0), (-1,-1), 6),
         ("BACKGROUND", (0,1), (-1,-1), WHITE)])

    y3 -= 12
    round_rect(c, M, y3 - 56, CONTENT_W, 56, CREAM, None, 8)
    para(c,
        "<b>Beta thesis:</b> users will open a dedicated account, sustain recurring installments, value the interest and deadline intelligence, and pay for the automation. "
        "The 50-user beta is designed to prove or disprove those behaviors before GoalPilot expands into securities, lending, cards, or multiple goals.",
        styles["body"], M + 14, y3 - 10, CONTENT_W - 28)

    footer(c, 2)
    c.showPage()


def draw_page_3(c):
    header(c, "Product")
    y = page_title(
        c, "02", "The product is a control loop, not a savings bucket",
        "GoalPilot manages the path to a specific purchase, while the bank partner remains authoritative for the actual deposit account."
    )

    # Horizontal flow.
    para(c, "THE SET-AND-FORGET JOURNEY", styles["section_num"], M, y, CONTENT_W)
    y -= 22
    gap = 7
    sw = (CONTENT_W - 2 * gap) / 3
    sh = 86
    steps = [
        (1, "Define the goal", "Purchase, target amount, deadline, current savings, and flexibility."),
        (2, "Open the account", "Partner-led onboarding, identity verification, disclosures, and consent."),
        (3, "Authorize installments", "Amount, cadence, first date, limits, pause and cancellation terms."),
        (4, "Automate funding", "Upcoming reminder, ACH submission, settlement, returns, and retry controls."),
        (5, "Track progress", "Available balance, posted interest, remaining amount, and projected completion."),
        (6, "Complete the purchase", "Stop installments, withdraw to a verified bank, and optionally use a merchant offer."),
    ]
    for i, step in enumerate(steps):
        row = i // 3
        col = i % 3
        x = M + col * (sw + gap)
        yy = y - row * (sh + 10) - sh
        flow_step(c, x, yy, sw, sh, *step)
    y = y - 2 * (sh + 10) - 4

    # Example plan.
    para(c, "A CONSERVATIVE FUNDING EXAMPLE", styles["h2"], M, y, CONTENT_W)
    y -= 22
    round_rect(c, M, y - 130, 230, 130, PALE, LINE, 9, 0.7)
    example_data = [
        ("Goal", "Used motorcycle"),
        ("Target", "$8,000"),
        ("Current savings", "$1,000"),
        ("Deadline", "18 months"),
        ("Required installment at 0%", "$388.89 / month"),
        ("Recommended installment", "$390 / month"),
    ]
    yy = y - 16
    for label, val in example_data:
        c.setFillColor(SLATE)
        c.setFont("Helvetica", 7.7)
        c.drawString(M + 14, yy, label)
        c.setFillColor(INK)
        c.setFont("Helvetica-Bold", 8.5)
        c.drawRightString(M + 216, yy, val)
        c.setStrokeColor(LINE)
        c.line(M + 14, yy - 5, M + 216, yy - 5)
        yy -= 18

    para(c,
        "GoalPilot should not underfund a goal by assuming today’s variable APY will persist. The installment is set from a conservative baseline; interest becomes a buffer that may bring the completion date forward or absorb a price increase.",
        styles["body"], M + 252, y - 4, CONTENT_W - 252)
    para(c,
        "<b>Key design principle:</b> the user chooses the outcome and constraints. GoalPilot handles the transfer schedule, progress math, and exceptions. The experience should feel automatic without becoming opaque.",
        styles["callout"], M + 252, y - 74, CONTENT_W - 252)
    y -= 154

    # Automation rules.
    para(c, "AUTOMATION WITH EXPLICIT SAFETY BOUNDARIES", styles["h2"], M, y, CONTENT_W)
    y -= 20
    left_items = [
        "Send a clear reminder before each installment; permit a skip or pause.",
        "Create a durable transfer command before calling the provider.",
        "Wait for settlement before counting money as available toward the goal.",
        "Post interest only from provider-reported account activity.",
    ]
    right_items = [
        "Stop new contributions when the goal is funded, the account is restricted, or a return occurs.",
        "Never repeat an ambiguous transfer blindly; look up and reconcile the provider outcome.",
        "Allow withdrawals only to a verified account owned by the same user during beta.",
        "Provide global and account-specific transfer kill switches for operators.",
    ]
    y1 = bullet_list(c, left_items, M, y, 247, 8.2, 11.2, gap=4)
    y2 = bullet_list(c, right_items, M + 265, y, 247, 8.2, 11.2, gap=4)

    yb = min(y1, y2) - 8
    round_rect(c, M, yb - 52, CONTENT_W, 52, BRG, None, 8)
    para(c,
        "The customer sees a simple goal. The operating system underneath must be precise about authorization, settlement, reversals, interest, record ownership, and reconciliation.",
        ParagraphStyle("white_callout", fontName="Helvetica-Bold", fontSize=10.5, leading=14.5, textColor=WHITE, alignment=TA_CENTER),
        M + 18, yb - 12, CONTENT_W - 36)

    footer(c, 3)
    c.showPage()


def draw_page_4(c):
    header(c, "Market and positioning")
    y = page_title(
        c, "03", "A familiar behavior with a sharper customer job",
        "Banks have proven that consumers use labeled savings buckets. GoalPilot differentiates through deadline intelligence, automation, and purchase completion."
    )

    # Market narrative and wedge.
    left = 310
    y1 = para(c,
        "GoalPilot enters a broad, banked, mobile-first market. The FDIC found that 96% of U.S. households were banked in 2023 and that mobile banking was the primary access method for 48.3% of banked households.<super>1</super> "
        "The Federal Reserve reported that 94% of adults had a bank account in 2025.<super>2</super>",
        styles["body"], M, y, left)
    y1 -= 10
    y1 = para(c,
        "The relevant serviceable market is narrower: consumers planning a purchase of roughly $1,000 to $25,000 over 3 to 36 months who can fund it through predictable installments. The initial categories are electronics and hobbies, travel, vehicle down payments, furniture and appliances, weddings, and home improvement.",
        styles["body"], M, y1, left)

    # Wedge graphic.
    gx, gy, gw, gh = M + 332, y - 142, 180, 146
    round_rect(c, gx, gy, gw, gh, CREAM, None, 10)
    c.setFillColor(BRG)
    c.setFont("Helvetica-Bold", 8)
    c.drawString(gx + 16, gy + gh - 22, "THE WEDGE")
    labels = [
        ("Goal math", 0.92),
        ("Funding automation", 0.78),
        ("Interest-bearing custody", 0.64),
        ("Purchase completion", 0.50),
    ]
    yy = gy + gh - 45
    for lab, frac in labels:
        c.setFillColor(SLATE)
        c.setFont("Helvetica", 7.2)
        c.drawString(gx + 16, yy + 4, lab)
        c.setFillColor(MINT)
        c.roundRect(gx + 16, yy - 6, 146, 7, 3.5, fill=1, stroke=0)
        c.setFillColor(BRG_3)
        c.roundRect(gx + 16, yy - 6, 146 * frac, 7, 3.5, fill=1, stroke=0)
        yy -= 26

    y = min(y1, gy) - 14
    para(c, "COMPETITIVE POSITION", styles["h2"], M, y, CONTENT_W)
    y -= 20
    comp = [
        [P("Category", "table_head"), P("What it does well", "table_head"), P("Where GoalPilot extends the experience", "table_head")],
        [P("Bank savings buckets", "table_cell_bold"), P("Free, trusted, liquid, interest-bearing"), P("Purchase price, deadline math, adaptive installments, and completion workflow")],
        [P("Automated savings apps", "table_cell_bold"), P("Behavioral rules and recurring saving"), P("Dedicated interest-bearing account plus purchase-readiness intelligence")],
        [P("High-yield savings", "table_cell_bold"), P("Competitive yield and simple custody"), P("The user does not need to build and maintain the plan manually")],
        [P("Budgeting tools", "table_cell_bold"), P("Broad visibility into spending and accounts"), P("GoalPilot actually holds and grows dedicated goal funds through a partner bank")],
        [P("BNPL and personal loans", "table_cell_bold"), P("Immediate access to the purchase"), P("GoalPilot rewards patience and avoids financing the full purchase")],
    ]
    y = table(c, comp, [112, 170, 230], M, y,
        [("BACKGROUND", (0,0), (-1,0), BRG), ("GRID", (0,0), (-1,-1), 0.35, LINE),
         ("VALIGN", (0,0), (-1,-1), "TOP"), ("LEFTPADDING", (0,0), (-1,-1), 7),
         ("RIGHTPADDING", (0,0), (-1,-1), 7), ("TOPPADDING", (0,0), (-1,-1), 6),
         ("BOTTOMPADDING", (0,0), (-1,-1), 6), ("BACKGROUND", (0,1), (-1,-1), WHITE)])

    y -= 14
    para(c, "WHY GOALPILOT SHOULD NOT COMPETE ON APY ALONE", styles["h2"], M, y, CONTENT_W)
    y -= 20
    y = bullet_list(c, [
        "Rates are variable and can be copied or repriced by banks.",
        "The durable product is the combination of target, deadline, automation, accountability, and completion.",
        "Goal and contribution data create a product-learning flywheel without requiring the sale of identifiable financial data.",
        "Merchant and institutional distribution can reduce acquisition cost once the consumer behavior is proven."
    ], M, y, CONTENT_W, 8.6, 12, gap=4)

    y -= 8
    round_rect(c, M, y - 58, CONTENT_W, 58, PALE, LINE, 8, 0.6)
    para(c,
        "<b>Positioning:</b> GoalPilot is not another bank with folders. It is the operating system for a planned purchase, with an account embedded inside the workflow.",
        styles["callout"], M + 18, y - 13, CONTENT_W - 36)

    footer(c, 4)
    c.showPage()


def draw_page_5(c):
    header(c, "Business model")
    y = page_title(
        c, "04", "Three aligned revenue streams, one primary economic engine",
        "Membership should carry the business. Deposit-program and merchant revenue improve lifetime value without determining the customer recommendation."
    )

    # Revenue columns.
    gap = 9
    cw = (CONTENT_W - 2 * gap) / 3
    cards = [
        ("1", "Membership", "$4.99 monthly or $49 annually", "Automation, progress monitoring, one live account and one active goal."),
        ("2", "Deposit-program revenue", "Contractual balance economics", "A disclosed interest split or separate balance incentive, subject to provider and bank agreement."),
        ("3", "Purchase completion", "Affiliate or referral commission", "Paid by a merchant or marketplace when the user completes a goal through an approved partner."),
    ]
    for i, (n, title, price, detail) in enumerate(cards):
        x = M + i * (cw + gap)
        round_rect(c, x, y - 146, cw, 146, WHITE, LINE, 9, 0.7)
        c.setFillColor(BRG)
        c.circle(x + 17, y - 18, 9, fill=1, stroke=0)
        c.setFillColor(WHITE)
        c.setFont("Helvetica-Bold", 8)
        c.drawCentredString(x + 17, y - 20.5, n)
        para(c, title, styles["h3"], x + 32, y - 10, cw - 42)
        para(c, price, ParagraphStyle("price", fontName="Helvetica-Bold", fontSize=8.2, leading=11, textColor=GOLD), x + 12, y - 48, cw - 24)
        para(c, detail, styles["body_small"], x + 12, y - 70, cw - 24)
    y -= 164

    # Unit economics illustration.
    para(c, "ILLUSTRATIVE REVENUE PER ACTIVE FUNDED USER", styles["h2"], M, y, CONTENT_W)
    y -= 18
    para(c,
        "The following is a sensitivity illustration, not a forecast. Provider pricing and commercial terms are unknown and must be obtained before launch.",
        styles["body_muted"], M, y, CONTENT_W)
    y -= 23

    econ = [
        [P("Component", "table_head"), P("Illustrative assumption", "table_head"), P("Annual value", "table_head")],
        [P("Membership", "table_cell_bold"), P("$49 annual plan"), P("$49.00", "table_cell_bold")],
        [P("Balance economics", "table_cell_bold"), P("$2,500 average balance × 0.25% platform incentive"), P("$6.25", "table_cell_bold")],
        [P("Completion revenue", "table_cell_bold"), P("25% completion rate × $20 average commission"), P("$5.00", "table_cell_bold")],
        [P("Illustrative gross revenue", "table_cell_bold"), P("Before provider, payment, fraud, support, cloud and compliance costs"), P("$60.25", "table_cell_bold")],
    ]
    y = table(c, econ, [142, 277, 93], M, y,
        [("BACKGROUND", (0,0), (-1,0), BRG), ("GRID", (0,0), (-1,-1), 0.35, LINE),
         ("VALIGN", (0,0), (-1,-1), "TOP"), ("LEFTPADDING", (0,0), (-1,-1), 8),
         ("RIGHTPADDING", (0,0), (-1,-1), 8), ("TOPPADDING", (0,0), (-1,-1), 7),
         ("BOTTOMPADDING", (0,0), (-1,-1), 7), ("BACKGROUND", (0,1), (-1,3), WHITE),
         ("BACKGROUND", (0,4), (-1,4), CREAM)])

    y -= 14
    # Scale sensitivity chart.
    para(c, "SCALE SENSITIVITY AT THE ILLUSTRATIVE $60.25 GROSS REVENUE", styles["h2"], M, y, CONTENT_W)
    y -= 23
    values = [("10,000 users", 0.6025), ("50,000 users", 3.0125), ("100,000 users", 6.025)]
    chart_x = M + 92
    chart_w = 390
    max_val = 6.2
    for idx, (lab, val) in enumerate(values):
        yy = y - idx * 31
        c.setFillColor(SLATE)
        c.setFont("Helvetica", 7.8)
        c.drawRightString(chart_x - 10, yy - 1, lab)
        c.setFillColor(MINT)
        c.roundRect(chart_x, yy - 8, chart_w, 13, 6.5, fill=1, stroke=0)
        c.setFillColor(BRG_3)
        bw = chart_w * (val / max_val)
        c.roundRect(chart_x, yy - 8, bw, 13, 6.5, fill=1, stroke=0)
        c.setFillColor(INK)
        c.setFont("Helvetica-Bold", 7.8)
        c.drawString(chart_x + bw + 7, yy - 2, f"${val:.2f}M")
    y -= 102

    # Formula and gate.
    round_rect(c, M, y - 82, CONTENT_W, 82, BRG, None, 10)
    para(c, "THE COMMERCIAL GO / NO-GO FORMULA", styles["cover_kicker"], M + 18, y - 14, CONTENT_W - 36)
    formula_style = ParagraphStyle("formula", fontName="Helvetica-Bold", fontSize=10.2, leading=14, textColor=WHITE, alignment=TA_CENTER)
    para(c,
        "Contribution margin = membership + balance economics + completion revenue − KYC − account servicing − ACH − data − support − fraud/returns − cloud",
        formula_style, M + 24, y - 36, CONTENT_W - 48)
    y -= 97

    para(c, "Commercial discipline", styles["h2"], M, y, CONTENT_W)
    y -= 20
    bullet_list(c, [
        "Do not subsidize free live accounts before provider-level variable costs are known.",
        "Do not rely on interest-rate spread as the primary revenue source; it compresses when rates fall.",
        "Keep merchant compensation separate from goal logic and clearly disclose sponsored placement.",
        "Require the provider RFP to demonstrate a credible path to positive contribution margin at modest scale."
    ], M, y, CONTENT_W, 8.3, 11.4, gap=3)

    footer(c, 5)
    c.showPage()


def draw_page_6(c):
    header(c, "Operating and entity model")
    y = page_title(
        c, "05", "GoalPilot owns the experience, not the customer deposits",
        "The recommended structure keeps customer money at a named FDIC-insured bank and keeps GoalPilot’s operating funds entirely separate."
    )

    # Legal/operating model diagram.
    para(c, "RECOMMENDED ENTITY AND CONTRACT MODEL", styles["h2"], M, y, CONTENT_W)
    y -= 18
    boxes = {
        "user": (M, y - 105, 105, 72, "User", "Owns the goal and deposit account"),
        "gp": (M + 145, y - 105, 125, 72, "GoalPilot, Inc.", "Software, membership, automation and support"),
        "platform": (M + 310, y - 105, 125, 72, "Embedded-finance platform", "APIs, operations and sponsor-bank integration"),
        "bank": (M + 455, y - 105, 57, 72, "Bank", "Account and custody"),
    }
    for key, (x, yy, w, h, title, detail) in boxes.items():
        fill = BRG if key == "gp" else WHITE
        stroke = BRG if key in ("gp", "bank") else LINE
        round_rect(c, x, yy, w, h, fill, stroke, 8, 0.8)
        tstyle = ParagraphStyle("bt"+key, fontName="Helvetica-Bold", fontSize=8.2, leading=10, textColor=WHITE if key == "gp" else BRG, alignment=TA_CENTER)
        dstyle = ParagraphStyle("bd"+key, fontName="Helvetica", fontSize=6.6, leading=8.2, textColor=HexColor("#DDE8E1") if key == "gp" else SLATE, alignment=TA_CENTER)
        para(c, title, tstyle, x + 6, yy + h - 12, w - 12)
        para(c, detail, dstyle, x + 8, yy + h - 35, w - 16)
    # connectors
    c.setStrokeColor(GOLD)
    c.setLineWidth(1.4)
    for x1, x2 in [(M + 105, M + 145), (M + 270, M + 310), (M + 435, M + 455)]:
        c.line(x1 + 3, y - 69, x2 - 3, y - 69)
        c.line(x2 - 8, y - 73, x2 - 3, y - 69)
        c.line(x2 - 8, y - 65, x2 - 3, y - 69)
    c.setFillColor(SLATE)
    c.setFont("Helvetica", 5.7)
    c.drawCentredString(M + 125, y - 57, "membership")
    c.drawCentredString(M + 290, y - 57, "program agreement")
    c.drawCentredString(M + 445, y - 57, "banking services")
    y -= 126

    # Structure recommendations.
    left_w = 250
    para(c, "Corporate recommendation", styles["h2"], M, y, left_w)
    yy1 = y - 20
    yy1 = para(c,
        "Use a single operating company, preferably a Delaware C corporation if venture financing is expected. GoalPilot, Inc. owns the product, contracts, brand and intellectual property; bills membership; and receives affiliate or program revenue. It does not place customer deposits into its operating account.",
        styles["body"], M, yy1, left_w)
    yy1 -= 10
    yy1 = para(c,
        "A future GoalPilot Advisory LLC or partner-RIA arrangement can be added if the company later introduces Treasury or securities portfolios. It is unnecessary for the deposit-only launch product.",
        styles["body"], M, yy1, left_w)

    para(c, "Account-ownership hierarchy", styles["h2"], M + 270, y, 242)
    yy2 = y - 20
    hierarchy = [
        "<b>Preferred:</b> individual customer-owned deposit account at the partner bank.",
        "<b>Fallback:</b> bank-owned FBO structure with customer-level bank records and daily reconciliation.",
        "<b>Rejected for beta:</b> GoalPilot-owned pooled customer-funds account or commingled operating account."
    ]
    yy2 = bullet_list(c, hierarchy, M + 270, yy2, 242, 8.2, 11.4, gap=5)

    y = min(yy1, yy2) - 14
    round_rect(c, M, y - 68, CONTENT_W, 68, CREAM, None, 8)
    para(c,
        "<b>Deposit-insurance language must be exact.</b> GoalPilot itself is not FDIC insured. Eligible deposits may be insured through the named partner bank, subject to account ownership, recordkeeping, deposit category, aggregation with the customer’s other deposits at that bank, and other FDIC requirements.<super>7,11</super>",
        styles["body"], M + 16, y - 13, CONTENT_W - 32)
    y -= 83

    para(c, "REFERENCE PROVIDER SHORTLIST", styles["h2"], M, y, CONTENT_W)
    y -= 20
    provider = [
        [P("Provider", "table_head"), P("Why it is relevant", "table_head"), P("What must be confirmed", "table_head")],
        [P("Unit", "table_cell_bold"), P("Consumer-account focus; interest-bearing account support; interest can be shared or retained under program terms<super>4</super>"), P("Sponsor bank, production acceptance, pricing, account ownership, compliance allocation, minimums")],
        [P("Increase", "table_cell_bold"), P("Customer accounts; bank-paid interest and separate account-revenue or balance-incentive payments<super>5</super>"), P("Consumer-program suitability, support model, sponsor bank, economics, minimum commitment")],
        [P("Treasury Prime", "table_cell_bold"), P("Bank-direct embedded-finance platform and account APIs"), P("Small-beta willingness, consumer support, interest economics, pricing, and bank selection")],
    ]
    y = table(c, provider, [75, 218, 219], M, y,
        [("BACKGROUND", (0,0), (-1,0), BRG), ("GRID", (0,0), (-1,-1), 0.35, LINE),
         ("VALIGN", (0,0), (-1,-1), "TOP"), ("LEFTPADDING", (0,0), (-1,-1), 7),
         ("RIGHTPADDING", (0,0), (-1,-1), 7), ("TOPPADDING", (0,0), (-1,-1), 6),
         ("BOTTOMPADDING", (0,0), (-1,-1), 6), ("BACKGROUND", (0,1), (-1,-1), WHITE)])

    y -= 12
    para(c,
        "Provider names are reference candidates, not selections. A provider RFP and direct contract review must precede material build-out because public API documentation does not establish commercial availability, sponsor-bank approval, pricing, or responsibility allocation.",
        styles["body_muted"], M, y, CONTENT_W)

    footer(c, 6)
    c.showPage()


def draw_page_7(c):
    header(c, "Platform and data model")
    y = page_title(
        c, "06", "A lean architecture with bank-grade workflow discipline",
        "GoalPilot’s database owns the goal and automation workflow. The provider and bank remain authoritative for balances, transfers, interest, and statements."
    )

    # Architecture diagram.
    para(c, "LEAN PRODUCTION ARCHITECTURE", styles["h2"], M, y, CONTENT_W)
    y -= 18
    diagram_y = y - 138
    round_rect(c, M, diagram_y, CONTENT_W, 138, PALE, LINE, 10, 0.6)
    nodes = [
        (M + 14, diagram_y + 76, 76, 42, "React web", "Customer UI"),
        (M + 105, diagram_y + 76, 84, 42, "CloudFront", "WAF + TLS"),
        (M + 204, diagram_y + 76, 84, 42, "Fastify API", "BFF + auth"),
        (M + 303, diagram_y + 76, 84, 42, "PostgreSQL", "Workflow truth"),
        (M + 402, diagram_y + 76, 94, 42, "Financial queue", "FIFO commands"),
        (M + 204, diagram_y + 18, 84, 42, "Worker", "Transfers + jobs"),
        (M + 303, diagram_y + 18, 84, 42, "Provider API", "Accounts + ACH"),
        (M + 402, diagram_y + 18, 94, 42, "Partner bank", "Deposits + interest"),
    ]
    for x, yy, w, h, title, detail in nodes:
        dark = title in ("Fastify API", "PostgreSQL", "Worker")
        round_rect(c, x, yy, w, h, BRG if dark else WHITE, BRG if dark else LINE, 7, 0.6)
        st1 = ParagraphStyle("n1"+title, fontName="Helvetica-Bold", fontSize=7.2, leading=8.5, textColor=WHITE if dark else BRG, alignment=TA_CENTER)
        st2 = ParagraphStyle("n2"+title, fontName="Helvetica", fontSize=5.8, leading=7, textColor=HexColor("#DDE8E1") if dark else SLATE, alignment=TA_CENTER)
        para(c, title, st1, x + 4, yy + h - 9, w - 8)
        para(c, detail, st2, x + 4, yy + 15, w - 8)
    # arrows top row
    c.setStrokeColor(GOLD)
    c.setLineWidth(1.2)
    for x1, x2 in [(M+90, M+105), (M+189, M+204), (M+288, M+303), (M+387, M+402)]:
        c.line(x1+2, diagram_y+97, x2-2, diagram_y+97)
    # down / bottom arrows
    c.line(M+246, diagram_y+76, M+246, diagram_y+60)
    c.line(M+288, diagram_y+39, M+303, diagram_y+39)
    c.line(M+387, diagram_y+39, M+402, diagram_y+39)
    c.setFillColor(SLATE)
    c.setFont("Helvetica", 5.5)
    c.drawString(M + 15, diagram_y + 7, "Provider webhooks → verified inbox → legal state transition → reconciliation")
    y = diagram_y - 14

    # Entity model table.
    para(c, "RECOMMENDED PLATFORM ENTITY MODEL", styles["h2"], M, y, CONTENT_W)
    y -= 19
    entities = [
        [P("Entity", "table_head"), P("Purpose", "table_head"), P("Authority", "table_head")],
        [P("User / PartnerCustomer", "table_cell_bold"), P("GoalPilot identity mapped to provider customer"), P("GoalPilot mapping; provider identity status")],
        [P("Goal / GoalProjection", "table_cell_bold"), P("Target, deadline, installment, progress and forecast"), P("GoalPilot")],
        [P("GoalAccount", "table_cell_bold"), P("Reference to the customer-owned partner-bank account"), P("Bank/provider")],
        [P("FundingSource", "table_cell_bold"), P("Tokenized verified external bank connection"), P("Provider and linked institution")],
        [P("ContributionPlan / TransferAuthorization", "table_cell_bold"), P("Cadence, amount, limits, consent and cancellation"), P("GoalPilot evidence; provider execution")],
        [P("Transfer / InterestPosting / BalanceSnapshot", "table_cell_bold"), P("Installment state, posted interest and account balances"), P("Bank/provider")],
        [P("ProviderEvent / ReconciliationRun", "table_cell_bold"), P("Immutable webhook intake and provider-vs-local comparison"), P("Provider event; GoalPilot control record")],
        [P("ConsentRecord / AuditEvent", "table_cell_bold"), P("Terms, disclosures, authorization and security-safe history"), P("GoalPilot")],
    ]
    y = table(c, entities, [148, 235, 129], M, y,
        [("BACKGROUND", (0,0), (-1,0), BRG), ("GRID", (0,0), (-1,-1), 0.32, LINE),
         ("VALIGN", (0,0), (-1,-1), "TOP"), ("LEFTPADDING", (0,0), (-1,-1), 6),
         ("RIGHTPADDING", (0,0), (-1,-1), 6), ("TOPPADDING", (0,0), (-1,-1), 4.6),
         ("BOTTOMPADDING", (0,0), (-1,-1), 4.6), ("BACKGROUND", (0,1), (-1,-1), WHITE)])

    y -= 11
    para(c, "NON-NEGOTIABLE OPERATING CONTROLS", styles["h2"], M, y, CONTENT_W)
    y -= 19
    controls = [
        "Durable transfer record and idempotency key before provider submission.",
        "Signed webhook verification, deduplication, raw-event retention, and replay protection.",
        "Daily reconciliation of transfers, balances, interest and account status.",
        "No blind retry after an ambiguous provider timeout; query and reconcile first.",
        "Separate GoalPilot operating funds from customer deposits at all times.",
        "Encrypted provider credentials, strict log redaction, and operator kill switches."
    ]
    bullet_list(c, controls, M, y, CONTENT_W, 8.1, 11, gap=2)

    footer(c, 7)
    c.showPage()


def draw_page_8(c):
    header(c, "Go-to-market and beta")
    y = page_title(
        c, "07", "Prove recurring behavior before scaling the financial program",
        "The first 50 users are an operating experiment, not a growth campaign. The objective is to validate funding persistence, willingness to pay, and safe account operations."
    )

    # ICP and wedge.
    left_w = 250
    para(c, "INITIAL CUSTOMER PROFILE", styles["h2"], M, y, left_w)
    yy1 = y - 20
    yy1 = bullet_list(c, [
        "Banked, mobile-first U.S. adult.",
        "Planned purchase of roughly $1,000 to $25,000.",
        "Target date approximately 3 to 36 months away.",
        "Predictable contribution capacity and preference to avoid full-cost borrowing.",
        "Strong emotional attachment to a specific goal."
    ], M, yy1, left_w, 8.4, 11.6, gap=4)

    para(c, "INITIAL GOAL CATEGORIES", styles["h2"], M + 270, y, 242)
    yy2 = y - 20
    categories = [
        ("Electronics and hobbies", "Exact price, sale alerts, affiliate fit"),
        ("Travel", "Fixed date, high motivation, booking partners"),
        ("Vehicle down payment", "Longer funding cycle and financing comparison"),
        ("Furniture and appliances", "Defined merchant and purchase event"),
        ("Weddings and events", "Deadline and shared contribution potential"),
    ]
    for title, detail in categories:
        c.setFillColor(BRG)
        c.setFont("Helvetica-Bold", 7.7)
        c.drawString(M + 270, yy2 - 2, title)
        c.setFillColor(SLATE)
        c.setFont("Helvetica", 6.7)
        c.drawString(M + 270, yy2 - 13, detail)
        yy2 -= 27

    y = min(yy1, yy2) - 10
    para(c, "BETA COHORT DESIGN", styles["h2"], M, y, CONTENT_W)
    y -= 23
    phases = [
        ("2", "Internal", "End-to-end production path with company-controlled accounts"),
        ("10", "Canary", "Daily review of onboarding, ACH, support and reconciliation"),
        ("25", "Intermediate", "Validate retention, support burden and early economics"),
        ("50", "Full beta", "Explicit approval only after operational thresholds hold"),
    ]
    fw = (CONTENT_W - 3 * 10) / 4
    for i, (num, title, desc) in enumerate(phases):
        x = M + i * (fw + 10)
        round_rect(c, x, y - 108, fw, 108, WHITE, LINE, 8, 0.7)
        c.setFillColor(BRG)
        c.setFont("Helvetica-Bold", 21)
        c.drawCentredString(x + fw/2, y - 31, num)
        c.setFillColor(GOLD)
        c.setFont("Helvetica-Bold", 7.5)
        c.drawCentredString(x + fw/2, y - 48, title.upper())
        para(c, desc, ParagraphStyle("phase"+title, fontName="Helvetica", fontSize=6.5, leading=8.2, textColor=SLATE, alignment=TA_CENTER), x + 8, y - 62, fw - 16)
    y -= 128

    para(c, "BETA SUCCESS SCORECARD", styles["h2"], M, y, CONTENT_W)
    y -= 19
    score = [
        [P("Metric", "table_head"), P("Decision threshold", "table_head"), P("What it proves", "table_head")],
        [P("Account-opening completion", "table_cell_bold"), P("≥ 60% of invited users"), P("The proposition and onboarding are credible")],
        [P("Installment activation", "table_cell_bold"), P("≥ 70% of opened accounts"), P("Users are willing to automate funding")],
        [P("Three consecutive installments", "table_cell_bold"), P("≥ 50% of activated users"), P("Behavior persists beyond novelty")],
        [P("90-day funded-account retention", "table_cell_bold"), P("≥ 65%"), P("The account remains useful")],
        [P("Trial-to-paid conversion", "table_cell_bold"), P("≥ 20%"), P("Membership can support unit economics")],
        [P("Customer-fund discrepancies", "table_cell_bold"), P("Zero unresolved > 1 business day"), P("Operating controls are effective")],
        [P("Severe security/privacy incidents", "table_cell_bold"), P("Zero"), P("The beta remains within risk tolerance")],
    ]
    y = table(c, score, [188, 127, 197], M, y,
        [("BACKGROUND", (0,0), (-1,0), BRG), ("GRID", (0,0), (-1,-1), 0.32, LINE),
         ("VALIGN", (0,0), (-1,-1), "TOP"), ("LEFTPADDING", (0,0), (-1,-1), 6),
         ("RIGHTPADDING", (0,0), (-1,-1), 6), ("TOPPADDING", (0,0), (-1,-1), 4.8),
         ("BOTTOMPADDING", (0,0), (-1,-1), 4.8), ("BACKGROUND", (0,1), (-1,-1), WHITE)])

    y -= 12
    para(c, "Acquisition sequence", styles["h2"], M, y, CONTENT_W)
    y -= 19
    bullet_list(c, [
        "Founder network and high-touch interviews to establish the first cohort.",
        "Goal-specific content for travel, vehicles, electronics and hobbies, rather than generic financial education.",
        "User referrals through privacy-safe progress cards that reveal no balance unless explicitly selected.",
        "Merchant and employer distribution only after GoalPilot proves recurring funding and retention."
    ], M, y, CONTENT_W, 8.2, 11.2, gap=3)

    footer(c, 8)
    c.showPage()


def draw_page_9(c):
    header(c, "Execution plan and risk")
    y = page_title(
        c, "08", "De-risk the bank program before committing to scale",
        "The critical path begins with commercial and legal validation, then proceeds through simulation, provider sandbox, production readiness, and a controlled beta."
    )

    # Timeline.
    para(c, "MILESTONE ROADMAP", styles["h2"], M, y, CONTENT_W)
    y -= 24
    timeline = [
        ("0", "Commercial validation", "4–8 weeks", "Provider RFP, sponsor-bank model, pricing, legal scope, customer interviews"),
        ("1", "Simulated product", "6–8 weeks", "Complete lifecycle with fake money, interest, transfers, exceptions and operator tools"),
        ("2", "Provider sandbox", "8–12 weeks", "KYC, accounts, ACH, interest, webhooks, statements and reconciliation"),
        ("3", "Production readiness", "4–8 weeks", "Security, disclosures, incident response, monitoring, restore and training"),
        ("4", "Controlled beta", "Cohort gated", "2 internal → 10 → 25 → 50 users, with explicit expansion approval"),
    ]
    xline = M + 20
    c.setStrokeColor(MINT)
    c.setLineWidth(4)
    c.line(xline, y - 12, xline, y - 216)
    yy = y
    for n, title, duration, detail in timeline:
        c.setFillColor(BRG)
        c.circle(xline, yy - 11, 9, fill=1, stroke=0)
        c.setFillColor(WHITE)
        c.setFont("Helvetica-Bold", 7.5)
        c.drawCentredString(xline, yy - 13.5, n)
        c.setFillColor(INK)
        c.setFont("Helvetica-Bold", 8.5)
        c.drawString(xline + 20, yy - 5, title)
        c.setFillColor(GOLD)
        c.setFont("Helvetica-Bold", 6.8)
        c.drawRightString(W - M, yy - 5, duration)
        para(c, detail, styles["body_small"], xline + 20, yy - 18, CONTENT_W - 40)
        yy -= 48
    y = yy - 4

    # Risk table.
    para(c, "PRINCIPAL RISKS AND MITIGATIONS", styles["h2"], M, y, CONTENT_W)
    y -= 19
    risks = [
        [P("Risk", "table_head"), P("Consequence", "table_head"), P("Mitigation", "table_head")],
        [P("Provider will not support an early consumer program", "table_cell_bold"), P("Core product cannot launch"), P("Run RFP before material integration work; keep provider ports and simulators")],
        [P("Account and transfer costs exceed membership economics", "table_cell_bold"), P("Negative gross margin"), P("Paid account from launch; contract-level unit economics gate")],
        [P("Rates fall", "table_cell_bold"), P("Lower consumer yield and deposit revenue"), P("Subscription-led economics; sell automation and completion, not APY")],
        [P("ACH returns, fraud or duplicate transfers", "table_cell_bold"), P("Loss, complaints and customer harm"), P("Limits, reminders, idempotency, no blind retry, reconciliation and kill switches")],
        [P("Partner or middleware failure", "table_cell_bold"), P("Customer access and record risk"), P("Customer-owned accounts, bank-level records, export rights and tested reconciliation")],
        [P("Free bank buckets are sufficient", "table_cell_bold"), P("Low willingness to pay"), P("Test deadline intelligence, adaptive automation and completion value in the beta")],
        [P("FDIC or APY language is misleading", "table_cell_bold"), P("Regulatory and trust damage"), P("Named-bank disclosures, legal review and Truth in Savings controls<super>9,11</super>")],
    ]
    y = table(c, risks, [170, 120, 222], M, y,
        [("BACKGROUND", (0,0), (-1,0), BRG), ("GRID", (0,0), (-1,-1), 0.32, LINE),
         ("VALIGN", (0,0), (-1,-1), "TOP"), ("LEFTPADDING", (0,0), (-1,-1), 5.5),
         ("RIGHTPADDING", (0,0), (-1,-1), 5.5), ("TOPPADDING", (0,0), (-1,-1), 4.2),
         ("BOTTOMPADDING", (0,0), (-1,-1), 4.2), ("BACKGROUND", (0,1), (-1,-1), WHITE)])

    y -= 11
    round_rect(c, M, y - 68, CONTENT_W, 68, BRG, None, 9)
    para(c, "THE NEXT CAPITAL DECISION", styles["cover_kicker"], M + 16, y - 13, CONTENT_W - 32)
    para(c,
        "A responsible financing target cannot be set from public documentation alone. The amount should be established after provider quotes, sponsor-bank requirements, legal scope, minimum commitments and the desired runway are known. The first investor milestone is therefore a bankable commercial package, not a speculative funding ask.",
        ParagraphStyle("white_body", fontName="Helvetica", fontSize=8.2, leading=11.3, textColor=WHITE),
        M + 16, y - 34, CONTENT_W - 32)

    footer(c, 9)
    c.showPage()


def draw_page_10(c):
    header(c, "Diligence and sources")
    y = page_title(
        c, "09", "What must be true, and what is already supported",
        "This plan separates sourced facts from assumptions that require contracts, counsel, or direct customer evidence."
    )

    para(c, "PRIORITY DILIGENCE QUESTIONS", styles["h2"], M, y, CONTENT_W)
    y -= 20
    qdata = [
        [P("Workstream", "table_head"), P("Question that must be answered", "table_head"), P("Evidence required", "table_head")],
        [P("Provider and bank", "table_cell_bold"), P("Can each beta user own an individual consumer deposit account with recurring ACH and interest?"), P("Written proposal, sponsor-bank confirmation and sandbox access")],
        [P("Economics", "table_cell_bold"), P("What are KYC, account, ACH, data, statement, support and minimum-commitment costs?"), P("Rate card and contract model")],
        [P("Responsibility", "table_cell_bold"), P("Who owns KYC, AML, Reg E error resolution, complaints, statements and 1099-INT?"), P("Responsibility matrix reviewed by counsel")],
        [P("Ownership and insurance", "table_cell_bold"), P("How are legal ownership, bank records and deposit-insurance disclosures structured?"), P("Account agreement and bank-approved disclosure language")],
        [P("Customer evidence", "table_cell_bold"), P("Will users open, fund, retain and pay for the account?"), P("50-user beta data against the scorecard")],
        [P("Continuity", "table_cell_bold"), P("Can GoalPilot export data and migrate accounts if the platform or bank relationship ends?"), P("Contractual data rights and tested exit plan")],
    ]
    y = table(c, qdata, [96, 256, 160], M, y,
        [("BACKGROUND", (0,0), (-1,0), BRG), ("GRID", (0,0), (-1,-1), 0.32, LINE),
         ("VALIGN", (0,0), (-1,-1), "TOP"), ("LEFTPADDING", (0,0), (-1,-1), 5.5),
         ("RIGHTPADDING", (0,0), (-1,-1), 5.5), ("TOPPADDING", (0,0), (-1,-1), 4.8),
         ("BOTTOMPADDING", (0,0), (-1,-1), 4.8), ("BACKGROUND", (0,1), (-1,-1), WHITE)])

    y -= 13
    para(c, "SOURCE NOTES", styles["h2"], M, y, CONTENT_W)
    y -= 16
    sources = [
        ("1", "FDIC, 2023 National Survey of Unbanked and Underbanked Households. 96% of U.S. households were banked; 48.3% of banked households used mobile banking as their primary access method. https://www.fdic.gov/news/press-releases/2024/fdic-survey-finds-96-percent-us-households-were-banked-2023"),
        ("2", "Board of Governors of the Federal Reserve System, Economic Well-Being of U.S. Households in 2025. 94% of adults had a bank account; 63% could cover a hypothetical $400 expense using cash or its equivalent. https://www.federalreserve.gov/publications/2026-economic-well-being-of-us-households-in-2025-executive-summary.htm"),
        ("3", "SoFi, Automatic Savings. Vaults, recurring transfers and automatic savings demonstrate established consumer behavior around labeled goals. https://www.sofi.com/banking/automatic-savings/"),
        ("4", "Unit, Interest documentation. Unit describes daily-balance interest calculations, monthly distribution, and contractual allocation between platform and customers. https://www.unit.co/docs/accounts/interest/"),
        ("5", "Increase, Interest and Referral Bonus. Increase describes bank-paid interest and separate deposit bonus or balance-incentive payments. https://increase.com/documentation/interest-and-referral-bonus"),
        ("6", "Plaid, Processor Tokens and Unit Auth Integration. Processor tokens can be passed to supported partners, including Unit, to retrieve account details. https://plaid.com/docs/api/processors/ and https://plaid.com/docs/auth/partnerships/unit/"),
        ("7", "FDIC, Pass-Through Deposit Insurance Coverage. Coverage depends on satisfying ownership and recordkeeping requirements. https://www.fdic.gov/resources/deposit-insurance/brochures/insured-deposits"),
        ("8", "CFPB, Regulation E §1005.10. Recurring electronic debits require a signed or similarly authenticated authorization and a copy for the consumer. https://www.consumerfinance.gov/rules-policy/regulations/1005/10/"),
        ("9", "CFPB, Regulation DD / Truth in Savings. Deposit disclosures cover APY, interest, minimum balances, terms, fees and advertising. https://www.consumerfinance.gov/rules-policy/regulations/1030/"),
        ("10", "Federal bank regulators, Joint Statement on Third-Party Deposit Products. Bank-fintech deposit arrangements require effective risk management and compliance controls. https://www.fdic.gov/news/press-releases/2024/agencies-remind-banks-potential-risks-associated-third-party-deposit"),
        ("11", "FDIC, Misrepresentations about Deposit Insurance. FDIC insurance covers eligible deposits at insured institutions and does not protect against nonbank failure or uninsured products. https://www.fdic.gov/news/press-releases/2023/pr23060.html"),
    ]
    # Two-column source layout.
    col_gap = 18
    col_w = (CONTENT_W - col_gap) / 2
    left_sources = sources[:ceil(len(sources)/2)]
    right_sources = sources[ceil(len(sources)/2):]
    y_left = y
    for n, text in left_sources:
        c.setFillColor(BRG)
        c.setFont("Helvetica-Bold", 7)
        c.drawString(M, y_left - 6, n)
        y_left = para(c, text, styles["source"], M + 14, y_left, col_w - 14)
        y_left -= 7
    y_right = y
    for n, text in right_sources:
        c.setFillColor(BRG)
        c.setFont("Helvetica-Bold", 7)
        c.drawString(M + col_w + col_gap, y_right - 6, n)
        y_right = para(c, text, styles["source"], M + col_w + col_gap + 14, y_right, col_w - 14)
        y_right -= 7

    yb = min(y_left, y_right) - 10
    round_rect(c, M, yb - 69, CONTENT_W, 69, CREAM, None, 8)
    para(c, "ASSUMPTIONS THAT ARE NOT YET VERIFIED", styles["section_num"], M + 15, yb - 12, CONTENT_W - 30)
    para(c,
        "Provider acceptance, sponsor-bank selection, APY, platform revenue share, provider pricing, minimum commitments, legal responsibility allocation, customer support ownership, and live production timelines are not established by public documentation. Financial examples in this plan are illustrative and must be replaced with contractual data.",
        styles["body_small"], M + 15, yb - 31, CONTENT_W - 30)

    c.setFillColor(BRG)
    c.setFont("Helvetica-Bold", 9)
    c.drawString(M, 48, "GoalPilot Cash")
    c.setFillColor(SLATE)
    c.setFont("Helvetica", 7)
    c.drawRightString(W - M, 48, "Earn interest instead of paying interest.")
    footer(c, 10)
    c.showPage()


def build_pdf():
    c = canvas.Canvas(str(PDF_PATH), pagesize=LETTER, pageCompression=1)
    c.setTitle("GoalPilot Cash Investor Business Plan")
    c.setAuthor("GoalPilot")
    c.setSubject("Investor business plan for an automated, interest-earning goal account")
    c.setKeywords("GoalPilot, fintech, savings, embedded finance, deposit account, investor business plan")

    draw_cover(c)
    draw_page_2(c)
    draw_page_3(c)
    draw_page_4(c)
    draw_page_5(c)
    draw_page_6(c)
    draw_page_7(c)
    draw_page_8(c)
    draw_page_9(c)
    draw_page_10(c)
    c.save()

    reader = PdfReader(str(PDF_PATH))
    if len(reader.pages) != 10:
        raise RuntimeError(f"Expected 10 pages, found {len(reader.pages)}")
    if PDF_PATH.stat().st_size < 50_000:
        raise RuntimeError(f"PDF unexpectedly small: {PDF_PATH.stat().st_size} bytes")
    extracted = "\n".join(page.extract_text() or "" for page in reader.pages)
    required = [
        "GoalPilot Cash", "interest-bearing", "RECOMMENDED ENTITY", "RECOMMENDED PLATFORM ENTITY MODEL",
        "MILESTONE ROADMAP", "SOURCE NOTES", "Provider acceptance"
    ]
    missing = [term for term in required if term.lower() not in extracted.lower()]
    if missing:
        raise RuntimeError(f"Required text missing from PDF extraction: {missing}")
    TEXT_PATH.write_text(
        f"Validated PDF: {PDF_PATH.name}\nPages: {len(reader.pages)}\nBytes: {PDF_PATH.stat().st_size}\n"
        f"Required text checks: PASS\n\nExtracted text sample:\n{extracted[:4000]}",
        encoding="utf-8"
    )
    print(f"Created {PDF_PATH} ({PDF_PATH.stat().st_size} bytes, {len(reader.pages)} pages)")


if __name__ == "__main__":
    build_pdf()
