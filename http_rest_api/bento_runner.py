import subprocess


def start_bento():
    print("Iniciando Bento automáticamente...")

    subprocess.Popen(
        ["bento", "-c", "bento.yaml"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
