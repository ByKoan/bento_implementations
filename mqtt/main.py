import subprocess
import time
import os
import signal

BENTO_PATH = r""      # Ruta a tu ejecutable
BENTO_CONFIG = r""   # Ruta a tu YAML

def start_bento():
    print("Iniciando Bento...")
    return subprocess.Popen(
        [BENTO_PATH, "-c", BENTO_CONFIG],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )

def main():
    while True:
        process = start_bento()

        try:
            for line in process.stdout:
                print(line.strip())

            process.wait()
            print("Bento se cerró. Reiniciando en 5 segundos...")

        except KeyboardInterrupt:
            print("Deteniendo Bento...")
            process.send_signal(signal.SIGINT)
            process.wait()
            break

        except Exception as e:
            print(f"Error: {e}")
            process.kill()

        time.sleep(5)


if __name__ == "__main__":
    main()
