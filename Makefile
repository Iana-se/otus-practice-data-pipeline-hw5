SHELL := /bin/bash

include .env

.EXPORT_ALL_VARIABLES:

.PHONY: generate-env
generate-env:
	@echo "Generating .env from infra/variables.json..."
	@python3 create_env.py
	@echo ".env файл успешно создан"

.PHONY: setup-airflow-variables
setup-airflow-variables:
	@echo "Running setup_airflow_variables.sh on $(AIRFLOW_HOST)..."
	ssh -i $(PRIVATE_KEY_PATH) \
		-o StrictHostKeyChecking=no \
		-o UserKnownHostsFile=/dev/null \
		$(AIRFLOW_VM_USER)@$(AIRFLOW_HOST) \
		'bash /home/ubuntu/setup_airflow_variables.sh'
	@echo "Script execution completed"

.PHONY: push-airflow-variables
push-airflow-variables:
	@echo "Pushing variables.json to Airflow API..."
	curl -X POST -H "Content-Type: application/json" \
		-u $(AIRFLOW_ADMIN_USER):$(AIRFLOW_ADMIN_PASSWORD) \
		--data-binary @variables.json \
		https://$(AIRFLOW_URL)/api/v1/variables
	@echo "Variables pushed successfully"

.PHONY: upload-dags-to-airflow
upload-dags-to-airflow:
	@echo "Uploading dags to $(AIRFLOW_HOST)..."
	scp -i $(PRIVATE_KEY_PATH) \
		-o StrictHostKeyChecking=no \
		-o UserKnownHostsFile=/dev/null \
		-r dags/*.py $(AIRFLOW_VM_USER)@$(AIRFLOW_HOST):/home/airflow/dags/
	@echo "Dags uploaded successfully"

.PHONY: upload-dags-to-bucket
upload-dags-to-bucket:
	@echo "Uploading dags to $(S3_BUCKET_NAME)..."
	s3cmd put --recursive dags/ s3://$(S3_BUCKET_NAME)/dags/
	@echo "DAGs uploaded successfully"

.PHONY: upload-src-to-bucket
upload-src-to-bucket:
	@echo "Uploading src to $(S3_BUCKET_NAME)..."
	s3cmd put --recursive src/ s3://$(S3_BUCKET_NAME)/src/
	@echo "Src uploaded successfully"

# .PHONY: upload-data-to-bucket
# upload-data-to-bucket:
# 	@echo "Uploading data to $(S3_BUCKET_NAME)..."
# 	s3cmd put --recursive data/input_data/*.csv s3://$(S3_BUCKET_NAME)/input_data/
# 	@echo "Data uploaded successfully"

# .PHONY: upload-data-to-bucket
# upload-data-to-bucket:
# 	@echo "Syncing all files from the source bucket to $(S3_BUCKET_NAME)..."
# 	@s3cmd sync --acl-public s3://otus-mlops-source-data/ s3://$(S3_BUCKET_NAME)/
# 	@echo "Syncing additional local files to $(S3_BUCKET_NAME)..."
# 	@s3cmd sync --acl-public ./local_files/ s3://$(S3_BUCKET_NAME)/
# 	@echo "Data successfully synced to $(S3_BUCKET_NAME)"


.PHONY: upload-data-to-bucket
upload-data-to-bucket:
	@echo "Syncing all files from the source bucket to $(S3_BUCKET_NAME)/input_data/..."
	@s3cmd sync --acl-public s3://otus-mlops-source-data/ s3://$(S3_BUCKET_NAME)/input_data/
	@echo "Syncing additional local files to $(S3_BUCKET_NAME)/input_data/..."
	@s3cmd sync --acl-public ./local_files/ s3://$(S3_BUCKET_NAME)/input_data/
	@echo "Data successfully synced to $(S3_BUCKET_NAME)/input_data/"

.PHONY: upload-data_10_files
upload-data_10_files:
	@echo "Syncing first 10 files from the source bucket to $(S3_BUCKET_NAME)/input_data/..."
	@for file in $$(s3cmd ls s3://otus-mlops-source-data/ | awk '{print $$4}' | head -n 10); do \
		s3cmd cp --acl-public $$file s3://$(S3_BUCKET_NAME)/input_data/; \
	done
	@echo "Syncing first 10 local files to $(S3_BUCKET_NAME)/input_data/..."
	@for file in $$(ls ./local_files | head -n 10); do \
		s3cmd put --acl-public ./local_files/$$file s3://$(S3_BUCKET_NAME)/input_data/; \
	done
	@echo "Data successfully synced (10 files from S3 + 10 local files)."



.PHONY: move-txt-to-input-data
move-txt-to-input-data:
	@echo "Moving only .txt files from bucket root to input_data/..."
	@s3cmd ls s3://$(S3_BUCKET_NAME)/ | awk '{print $$4}' | grep '\.txt$$' | xargs -I {} s3cmd mv {} s3://$(S3_BUCKET_NAME)/input_data/
	@echo ".txt files successfully moved to input_data/"


upload-all: upload-data-to-bucket upload-src-to-bucket upload-dags-to-bucket

.PHONY: clean-s3-bucket
clean-s3-bucket:
	@echo "Cleaning S3 bucket $(S3_BUCKET_NAME)..."
	s3cmd del --force --recursive s3://$(S3_BUCKET_NAME)/
	@echo "S3 bucket cleaned"

.PHONY: remove-s3-bucket
remove-s3-bucket:
	@echo "Removing S3 bucket $(S3_BUCKET_NAME)..."
	s3cmd rb s3://$(S3_BUCKET_NAME)
	@echo "S3 bucket removed"

.PHONY: download-output-data-from-bucket
download-output-data-from-bucket:
	@echo "Downloading output data from $(S3_BUCKET_NAME)..."
	s3cmd get --recursive s3://$(S3_BUCKET_NAME)/output_data/ data/output_data/
	@echo "Output data downloaded successfully"

.PHONY: instance-list
instance-list:
	@echo "Listing instances..."
	yc compute instance list

.PHONY: git-push-secrets
git-push-secrets:
	@echo "Pushing secrets to github..."
	python3 utils/push_secrets_to_github_repo.py

sync-repo:
	rsync -avz \
		--exclude=.venv \
		--exclude=infra/.terraform \
		--exclude=*.tfstate \
		--exclude=*.backup \
		--exclude=*.json . yc-proxy:/home/ubuntu/otus/otus-practice-data-pipeline

sync-env:
	rsync -avz yc-proxy:/home/ubuntu/otus/otus-practice-data-pipeline/.env .env

airflow-cluster-mon:
	yc logging read --group-name=default --follow