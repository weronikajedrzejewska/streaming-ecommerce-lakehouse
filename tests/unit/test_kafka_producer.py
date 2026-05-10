from src.producer.kafka_producer import make_event


def test_make_event_returns_expected_shape() -> None:
    event = make_event()

    assert event["event_id"]
    assert event["event_type"] in {"page_view", "add_to_cart", "purchase"}
    assert event["user_id"].startswith("user_")
    assert event["session_id"].startswith("session_")
    assert "payload" in event
    assert event["payload"]["product_id"].startswith("prod_")


def test_make_event_page_view_has_null_commerce_fields() -> None:
    page_view = None
    for _ in range(200):
        candidate = make_event()
        if candidate["event_type"] == "page_view":
            page_view = candidate
            break

    assert page_view is not None
    assert page_view["payload"]["price"] is None
    assert page_view["payload"]["currency"] is None
    assert page_view["payload"]["quantity"] is None
