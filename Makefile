# Run on Host
start:
	@docker build -t lab-extension-dev-image:latest .
	@docker stop lab-extension-dev-container
	@docker rm lab-extension-dev-container
	@docker run -d \
		--restart unless-stopped \
		--name lab-extension-dev-container \
		-p 8888:8888 \
		-v $(shell pwd):/home/jovyan/work/ \
		-w /home/jovyan/work/ \
		lab-extension-dev-image:latest

# Run inside dev container
setup: 
	@npm install
	@pip install -e .
	@jupyter labextension develop . --overwrite
	@jlpm run build

watch:
	@jlpm watch
	