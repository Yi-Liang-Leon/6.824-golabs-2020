lab1-master: clean
	go run -race mrmaster.go pg-*.txt

lab1-worker:
	go build -buildmode=plugin -race ../mrapps/wc.go 
	go run -race mrworker.go wc.so

clean: 
	rm -rf mr-*