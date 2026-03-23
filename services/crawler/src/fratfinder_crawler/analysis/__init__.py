from .embedded_data_detector import detect_embedded_data
from .page_analyzer import analyze_page
from .source_classifier import classify_source
from .strategy_selector import select_extraction_plan

__all__ = [
    "analyze_page",
    "classify_source",
    "detect_embedded_data",
    "select_extraction_plan",
]
