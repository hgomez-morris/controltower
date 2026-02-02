init-db:
	python scripts/init_db.py

run-sync:
	python scripts/run_sync.py

ui:
	streamlit run src/controltower/ui/app.py
