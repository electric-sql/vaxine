ifdef CI
	REBAR=$(shell whereis rebar3 | awk '{print $$2}')
else
	REBAR=$(shell pwd)/rebar3
endif

COVERPATH = $(shell pwd)/_build/test/cover
.PHONY: rel test relgentlerain docker-build docker-run

all: compile

compile: compile-vaxine compile-vax

compile-vaxine:
	$(REBAR) compile

compile-vax:
	make compile -C apps/vax

clean: clean-vaxine clean-vax

clean-vaxine:
	$(REBAR) clean

clean-vax:
	make clean -C apps/vax

distclean: clean relclean
	$(REBAR) clean --all

shell: rel
	export NODE_NAME=antidote@127.0.0.1 ; \
	export COOKIE=antidote ; \
	export ROOT_DIR_PREFIX=$$NODE_NAME/ ; \
	_build/default/rel/antidote/bin/antidote console ${ARGS}

rel:
	$(REBAR) release

relclean:
	rm -rf _build/default/rel

reltest: rel
	test/release_test.sh

# style checks
lint:
	${REBAR} lint
	${REBAR} fmt --check

check: distclean test reltest dialyzer lint

relgentlerain: export TXN_PROTOCOL=gentlerain
relgentlerain: relclean rel

relnocert: export NO_CERTIFICATION=true
relnocert: relclean rel

stage :
	$(REBAR) release -d

test: test-vaxine test-vax

ifdef EUNIT_MODS
EUNIT_MODULES=--module=${EUNIT_MODS}
endif
test-vaxine:
	${REBAR} eunit --verbose ${EUNIT_MODULES}

test-vax:
	make test -C apps/vax

proper:
	${REBAR} proper

coverage:
	${REBAR} cover --verbose

singledc:
ifdef SUITE
	${REBAR} ct --dir apps/antidote/test/singledc --verbose --suite ${SUITE}
else
	${REBAR} ct --dir apps/antidote/test/singledc --verbose --cover_export_name=singledc
endif

multidc: 
ifdef SUITE
	${REBAR} ct --dir apps/antidote/test/multidc --verbose --suite ${SUITE}
else
	${REBAR} ct --dir apps/antidote/test/multidc --verbose --cover_export_name=multidc

endif

systests: singledc multidc test_vx_ct

ifdef SUITE
SUITE_OPT=--suite ${SUITE}
endif

test_vx_ct:
	${REBAR} ct --dir apps/vx_server/test --cover --cover_export_name=vx_dc ${SUITE_OPT}

docs:
	${REBAR} doc skip_deps=true

xref: compile
	${REBAR} xref skip_deps=true

dialyzer: dialyzer-vaxine dialyzer-vax

dialyzer-vaxine:
	${REBAR} dialyzer

dialyzer-vax:
	make dialyzer -C apps/vax

CONTAINER_NAME=vaxine

docker-build:
	docker build -f Dockerfile.vaxine -t vaxine:local-build .

docker-run: docker-build
	docker run --rm -d --name ${CONTAINER_NAME} \
		-p "8087:8087" \
		-p "8088:8088" \
		vaxine:local-build

docker-clean:
ifneq ($(docker images -q vaxine:local-build 2> /dev/null), "")
	docker image rm -f vaxine:local-build
endif

docker-stop:
	docker stop ${CONTAINER_NAME}
