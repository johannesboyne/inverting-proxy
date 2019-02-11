package main

import (
	"net/http"

	"github.com/johannesboyne/inverting-proxy/proxy"
)

func main() {
	r := proxy.NewProxy(proxy.NewPersistentStore())
	http.ListenAndServe(":3333", r)
}
