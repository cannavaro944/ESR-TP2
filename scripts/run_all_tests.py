"""
scripts/run_all_tests.py — Runner único de todos os testes do projeto
=====================================================================
Executa:
  1. tests.test_standalone       (RTP, MJPEG loop, Forwarder, Pipeline, Checklist)
  2. tests.test_integration_ott  (Integração Controlo ott.py <-> Plano de Dados + Failover)
  3. Teste real de vídeo         (StreamServer + assets/movie.Mjpeg + VideoClient)

Uso:
  python scripts/run_all_tests.py
  python scripts/run_all_tests.py --no-gui
"""

import subprocess
import sys
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def _run_module(module_name: str) -> tuple:
    """Executa um módulo de teste via subprocess e devolve (ok, descricao)."""
    print(f"\n>>> A executar: python -m {module_name}")
    res = subprocess.run(
        [sys.executable, "-m", module_name],
        cwd=str(ROOT_DIR),
    )
    return res.returncode == 0, f"python -m {module_name}"


def _test_real_video_gui() -> tuple:
    """Testa a reprodução real de assets/movie.Mjpeg no VideoClient (2s)."""
    print("\n>>> A executar: Teste real StreamServer -> VideoClient (assets/movie.Mjpeg)")
    video_path = ROOT_DIR / "assets" / "movie.Mjpeg"
    if not video_path.is_file():
        print(f"[-] Ficheiro de vídeo não encontrado: {video_path}")
        return False, "Teste real VideoClient (assets/movie.Mjpeg em falta)"

    try:
        from src.dados.data_plane import StreamServer
        from src.dados.video_client import VideoClient

        srv = StreamServer(lambda sid: ["127.0.0.1"] if sid == "Stream1" else [])
        srv.start()
        srv.start_stream("Stream1", str(video_path), fps=20.0)

        vc = VideoClient("Stream1")
        vc._root.after(2000, vc._on_window_close)
        vc.run()
        srv.stop_all()

        stats = vc.stats
        print(f"[*] Estatísticas GUI: {stats}")
        ok = stats["frames_received"] > 0 and stats["frames_displayed"] > 0
        return ok, (
            f"VideoClient GUI ({stats['frames_received']} recebidos, "
            f"{stats['frames_displayed']} exibidos)"
        )
    except Exception as exc:
        print(f"[-] Erro no teste real de vídeo: {exc}")
        return False, f"VideoClient GUI ({exc})"


def main():
    no_gui = "--no-gui" in sys.argv[1:]

    print("=" * 70)
    print("  ESR TP2 — Execução Completa de Testes")
    print("=" * 70)

    checks = [
        _run_module("tests.test_standalone"),
        _run_module("tests.test_integration_ott"),
    ]
    if not no_gui:
        checks.append(_test_real_video_gui())

    print("\n" + "=" * 70)
    print("  RESUMO FINAL")
    print("=" * 70)
    passed = 0
    for ok, label in checks:
        tag = "[OK]    " if ok else "[FALHOU]"
        print(f"  {tag} {label}")
        if ok:
            passed += 1

    print(f"\n  Resultado: {passed}/{len(checks)} suites passaram.")
    print("=" * 70)
    sys.exit(0 if passed == len(checks) else 1)


if __name__ == "__main__":
    main()
