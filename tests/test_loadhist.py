from binance_announcements.loadhist import BINANCE_PULL_WARNING, parse_args, warn_binance_pull


def test_parse_args_defaults_to_jsonl():
    args = parse_args([])
    assert args.from_binance is False


def test_parse_args_from_binance():
    args = parse_args(["--from-binance"])
    assert args.from_binance is True


def test_binance_pull_warning_is_printed(capsys):
    warn_binance_pull()
    err = capsys.readouterr().err
    assert "警告" in err
    assert "JSONL_DIR" in err
    assert "不会绕过" in err
    assert BINANCE_PULL_WARNING.strip() in err
