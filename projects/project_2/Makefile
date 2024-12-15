SSB_SRC := ./ssb
SSB_DST := ./duckdb/benchmark/ssb

N_THREADS := 2

all: clean copy run

run:
	@echo running benchmarks
	./duckdb/build/release/benchmark/benchmark_runner --threads=$(N_THREADS) --out=sf_1_$(N_THREADS).out "benchmark/ssb/sf_1/.*"
	./duckdb/build/release/benchmark/benchmark_runner --threads=$(N_THREADS) --out=sf_10_$(N_THREADS).out "benchmark/ssb/sf_10/.*"
	./duckdb/build/release/benchmark/benchmark_runner --threads=$(N_THREADS) --out=sf_100_$(N_THREADS).out "benchmark/ssb/sf_100/.*"

copy:
	@echo copying ssb into benchmark folder
	cp -r $(SSB_SRC) $(SSB_DST)

clean:
	@echo removing ssb
	rm -rdf $(SSB_DST)/

