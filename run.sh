#!/usr/bin/env bash
set -euo pipefail

if [ -f .env ]; then
  set -a
  # Strip UTF-8 BOM if present and load .env
  . <(sed -e '1s/^\xEF\xBB\xBF//' .env)
  set +a
fi

if [ -f ".venv/Scripts/activate" ]; then
  # Windows Git Bash / MSYS
  . ".venv/Scripts/activate"
elif [ -f ".venv/bin/activate" ]; then
  # Unix-like
  . ".venv/bin/activate"
fi

PATH_SEP=":"
if [ "${OS:-}" = "Windows_NT" ] || [ -n "${MSYSTEM:-}" ]; then
  PATH_SEP=";"
fi
case "${OSTYPE:-}" in
  msys*|cygwin*) PATH_SEP=";" ;;
esac

if [ -n "${PYTHONPATH:-}" ]; then
  export PYTHONPATH="${PYTHONPATH}${PATH_SEP}$(pwd)/src"
else
  export PYTHONPATH="$(pwd)/src"
fi

if ! command -v streamlit >/dev/null 2>&1; then
  echo "streamlit no está disponible en el PATH. Activa tu entorno o instala dependencias." >&2
  exit 1
fi

streamlit run src/controltower/ui/app.py
