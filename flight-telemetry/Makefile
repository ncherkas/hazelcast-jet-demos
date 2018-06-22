# docker-grafana-graphite makefile

# Environment Variables
CONTAINER = grafana-dashboard

.PHONY: up

prep :
	mkdir -p \
		data/whisper \
		data/elasticsearch \
		data/grafana \
		log/graphite \
		log/graphite/webapp \
		log/elasticsearch

pull :
	docker-compose pull

#up : prep pull
#	docker-compose up -d

up :
	docker-compose up -d

down :
	docker-compose down

shell :
	docker exec -ti $(CONTAINER) /bin/bash

tail :
	docker logs -f $(CONTAINER)

clean :
	rm -rf data/whisper; \
	rm -rf data/elasticsearch; \
	rm -rf data/grafana; \
	rm -rf log/graphite; \
	rm -rf log/graphite/webapp; \
	rm -rf log/elasticsearch; \
	rm -rf log/supervisor;
