#! /bin/bash

ab -n 500000 -c 5000 -T application/json -p data.json http://localhost:8000/create/ > bench.out