"""
Crypto Pipeline  Run All Starter.

Streamlit app com botão "Run All" que executa sequencialmente:
  1. docker compose up -d
  2. Aguarda serviços saudáveis
  3. terraform init + apply (buckets MinIO)
  4. spark-submit bronze streaming
  5. Dashboard Streamlit (porta 8501)

Uso:  python run_all.py
"""

import subprocess
import time
import os
import sys

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
TERRAFORM_DIR = os.path.join(ROOT_DIR, "infra", "terraform")
DASHBOARD_SCRIPT = os.path.join(ROOT_DIR, "src", "dashboard", "app.py")
STARTER_PORT = "8502"

STEPS = [
    ("Docker Compose Up", "docker_compose_up"),
    ("Aguardar serviços saudáveis", "wait_healthy"),
    ("Terraform  Provisionar buckets", "terraform_apply"),
    ("Spark Streaming  Bronze", "spark_submit"),
    ("Dashboard Streamlit", "start_dashboard"),
]


def run_cmd(cmd, cwd=None, timeout=300):
    try:
        result = subprocess.run(
            cmd,
            cwd=cwd or ROOT_DIR,
            capture_output=True,
            text=True,
            timeout=timeout,
            shell=True,
        )
        output = (result.stdout or "") + (result.stderr or "")
        return result.returncode == 0, output.strip()
    except subprocess.TimeoutExpired:
        return False, "Timeout expirado"
    except Exception as e:
        return False, str(e)


def docker_compose_up():
    return run_cmd("docker compose up -d", timeout=180)


def wait_healthy():
    services = {
        "minio": False,
        "spark-master": False,
        "airflow-webserver": False,
    }
    deadline = time.time() + 90

    while time.time() < deadline:
        for svc in list(services):
            if services[svc]:
                continue
            ok, out = run_cmd(
                f'docker inspect --format="{{{{.State.Health.Status}}}}" {svc}',
                timeout=10,
            )
            if ok and "healthy" in out:
                services[svc] = True

        if all(services.values()):
            return True, "Todos os serviços estão healthy"

        time.sleep(5)

    pending = [s for s, ok in services.items() if not ok]
    return False, f"Timeout esperando: {', '.join(pending)}"


def terraform_apply():
    ok_init, out_init = run_cmd("terraform init -input=false", cwd=TERRAFORM_DIR, timeout=120)
    if not ok_init:
        return False, f"terraform init falhou:\n{out_init}"

    ok_apply, out_apply = run_cmd(
        "terraform apply -auto-approve -input=false",
        cwd=TERRAFORM_DIR,
        timeout=120,
    )
    return ok_apply, out_apply


def spark_submit():
    cmd = (
        "docker exec -d spark-master "
        "/opt/spark/bin/spark-submit "
        "--master local[1] "
        "/jobs/streaming/spark_bronze.py"
    )
    ok, out = run_cmd(cmd, timeout=30)
    if ok:
        return True, "Spark Streaming iniciado em background no container"
    return False, out


def start_dashboard():
    cmd = [
        sys.executable, "-m", "streamlit", "run",
        DASHBOARD_SCRIPT,
        "--server.port", "8501",
        "--server.headless", "true",
    ]
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=ROOT_DIR,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
        )
        time.sleep(3)
        if proc.poll() is None:
            return True, "Dashboard rodando em http://localhost:8501"
        return False, "Processo do dashboard encerrou inesperadamente"
    except Exception as e:
        return False, str(e)


STEP_FUNCS = {
    "docker_compose_up": docker_compose_up,
    "wait_healthy": wait_healthy,
    "terraform_apply": terraform_apply,
    "spark_submit": spark_submit,
    "start_dashboard": start_dashboard,
}


def streamlit_ui():
    import streamlit as st

    st.set_page_config(page_title="Crypto Pipeline  Starter", layout="centered")
    st.title("Crypto Pipeline  Starter")
    st.markdown("Executa todo o pipeline de ponta a ponta com um clique.")

    st.markdown("---")
    st.subheader("Etapas do Pipeline")

    for i, (label, _) in enumerate(STEPS, 1):
        st.markdown(f"**{i}.** {label}")

    st.markdown("---")

    if "running" not in st.session_state:
        st.session_state.running = False
        st.session_state.results = {}
        st.session_state.current_step = 0
        st.session_state.done = False

    def run_pipeline():
        st.session_state.running = True
        st.session_state.results = {}
        st.session_state.current_step = 0
        st.session_state.done = False

    col_run, _ = st.columns([1, 2])
    with col_run:
        st.button(
            "Run All",
            on_click=run_pipeline,
            disabled=st.session_state.running and not st.session_state.done,
            type="primary",
            use_container_width=True,
        )

    if st.session_state.running and not st.session_state.done:
        progress_bar = st.progress(0)
        status_container = st.container()

        for i, (label, key) in enumerate(STEPS):
            st.session_state.current_step = i + 1
            progress_bar.progress(i / len(STEPS))

            with status_container:
                with st.status(f"Executando: {label}...", expanded=True) as step_status:
                    st.write(f"Iniciando {label}...")
                    func = STEP_FUNCS[key]
                    ok, output = func()

                    st.session_state.results[key] = (ok, output)
                    truncated = output[-500:] if len(output) > 500 else output

                    if ok:
                        step_status.update(label=f"{label}  OK", state="complete")
                        st.code(truncated, language="text")
                    else:
                        step_status.update(label=f"{label}  ERRO", state="error")
                        st.error(truncated)
                        st.session_state.done = True
                        st.session_state.running = False
                        st.stop()

        progress_bar.progress(1.0)
        st.session_state.done = True
        st.session_state.running = False
        st.balloons()
        st.success("Pipeline completo! Dashboard disponivel em http://localhost:8501")

    elif st.session_state.done:
        st.markdown("### Resultados")
        for label, key in STEPS:
            if key in st.session_state.results:
                ok, output = st.session_state.results[key]
                icon = "OK" if ok else "ERRO"
                color = "green" if ok else "red"
                st.markdown(f":{color}[{icon}] **{label}**")
                if not ok:
                    truncated = output[-300:] if len(output) > 300 else output
                    st.error(truncated)

        all_ok = all(ok for ok, _ in st.session_state.results.values())
        if all_ok:
            st.success("Tudo rodando! Acesse o Dashboard em http://localhost:8501")

        st.markdown("---")
        st.markdown(
            "| Servico | URL |\n"
            "|---|---|\n"
            "| MinIO Console | http://localhost:9001 |\n"
            "| Spark Master UI | http://localhost:8085 |\n"
            "| Airflow | http://localhost:8080 |\n"
            "| Dashboard | http://localhost:8501 |"
        )

    st.markdown("---")
    st.caption("Crypto Pipeline  PUC Minas 2026/1")


if __name__ == "__main__":
    subprocess.run(
        [
            sys.executable, "-m", "streamlit", "run",
            __file__,
            "--server.port", STARTER_PORT,
            "--server.headless", "true",
        ],
        cwd=ROOT_DIR,
    )
else:
    streamlit_ui()
