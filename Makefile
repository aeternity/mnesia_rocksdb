suite=$(if $(SUITE), suite=$(SUITE), )
REBAR3=$(shell which rebar3 || echo ./rebar3)

.PHONY: all check test clean run dialyzer xref

all:
	$(REBAR3) compile

docs:
	$(REBAR3) doc

check:
	$(REBAR3) dialyzer

eunit:
	$(REBAR3) eunit $(suite)

ct:
	$(REBAR3) ct $(suite)

test: dialyzer xref eunit ct

dialyzer:
	$(REBAR3) dialyzer

xref:
	$(REBAR3) xref

conf_clean:
	@:

clean:
	$(REBAR3) clean
	$(RM) doc/*

run:
	$(REBAR3) shell
