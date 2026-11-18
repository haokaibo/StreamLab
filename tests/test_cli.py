from streamlab.cli import build_parser, main


def test_check_schemas_command_runs(capsys):
    assert main(["check-schemas"]) == 0
    assert "telemetry_sample.v1.avsc" in capsys.readouterr().out


def test_parser_has_expected_commands():
    parser = build_parser()
    for argv in (["check-schemas"], ["replay", "--file", "x"], ["verify", "--truth", "t"]):
        assert parser.parse_args(argv).command == argv[0]
