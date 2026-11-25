# Resonate protocol -- Tamarin analysis.
#
#   make check       parse and well-formedness, all four theories
#   make payload     confidentiality      -- expected 6/6 verified
#   make baseline    the control group    -- two attacks found, one falsification
#   make capability  the capability layer
#   make reference   the whole protocol, analysis configuration
#   make prove       all four, in that order
#   make gui         the interactive prover on http://localhost:3001

TAMARIN ?= tamarin-prover
MAIN    := models/resonate.spthy
BASE    := models/resonate-baseline.spthy
PAY     := models/resonate-payload.spthy
CAP     := models/resonate-capability.spthy
ORACLE  := oracles/resonate.oracle
export LANG   = C.UTF-8
export LC_ALL = C.UTF-8

.PHONY: check prove payload baseline capability reference fenced gui clean

check:
	@for m in $(PAY) $(CAP) $(BASE) $(MAIN); do \
	  printf '%-40s' $$m; $(TAMARIN) $$m --parse-only >/dev/null && echo OK; done

payload:    ; ./prove.sh $(PAY) 600 --derivcheck-timeout=60
baseline:   ; ./prove.sh $(BASE) 600 --derivcheck-timeout=60
capability: ; ./prove.sh $(CAP) 400 --derivcheck-timeout=60
reference:  ; ./prove.sh $(MAIN) 240 -D=Core -D=Reduced --derivcheck-timeout=60

prove: payload baseline capability reference

# The hardened settle path: drops Server_Promise_Settle_Targeted, so
# settlement_of_targeted_is_fenced replaces the attack lemma.
fenced:
	./prove.sh $(MAIN) 240 -D=Core -D=Reduced -D=FencedSettle --derivcheck-timeout=60

gui:
	$(TAMARIN) interactive models/ --heuristic=o --oraclename=$(ORACLE)

clean:
	rm -f *.dot *.png
