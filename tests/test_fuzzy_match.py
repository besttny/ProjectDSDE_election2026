from src.quality.fuzzy_match import best_match, normalize_text, suggest_value


def test_normalize_text_removes_whitespace_for_thai_matching():
    assert normalize_text("เพื่อ ไทย") == normalize_text("เพื่อไทย")


def test_best_match_returns_highest_similarity_candidate():
    match = best_match("เพือไทย", ["ประชาชน", "เพื่อไทย", "ชาติไทยพัฒนา"])

    assert match is not None
    assert match.value == "เพื่อไทย"
    assert match.score > 0.8


def test_suggest_value_marks_low_scores_as_no_confident_match():
    suggested, score, status = suggest_value("abc", ["เพื่อไทย"], threshold=0.86)

    assert suggested == "เพื่อไทย"
    assert score < 0.86
    assert status == "no_confident_match"
