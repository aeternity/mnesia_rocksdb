suite=$(if $(SUITE), suite=$(SUITE), )
REBAR3=$(shell which rebar3 || echo ./rebar3)

.PHONY: all check test clean run

all:
	$(REBAR3) compile

docs:
	$(REBAR3) doc

check:
	$(REBAR3) dialyzer

test:
	$(REBAR3) eunit $(suite)


conf_clean:
	@:

clean:
	$(REBAR3) clean
	$(RM) doc/*

run:
	$(REBAR3) shell
