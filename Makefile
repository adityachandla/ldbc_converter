.PHONY: converter basic_csr
converter:
	go build -o converter cmd/generic/main.go

basic_csr:
	go build -o basic_csr cmd/basic_csr/main.go

clean:
	rm -f converter
	rm -f basic_csr
	rm -rf adjacency
	rm -rf mapping
