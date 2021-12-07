build: docker-build
up: docker-up
down: docker-down
backend: docker-exec-backend

docker-down:
	docker-compose down --remove-orphans

docker-up:
	docker-compose up -d

docker-build:
	docker-compose build

composer:
	docker-compose exec backend composer install

docker-exec-backend:
	docker-compose exec backend bash