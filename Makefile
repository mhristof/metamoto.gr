clickhouse:
	docker run -v $(PWD)/data:/var/lib/clickhouse -p 8123:8123 --name clickhouse-server --rm -d yandex/clickhouse-server
