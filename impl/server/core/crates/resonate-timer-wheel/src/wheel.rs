//!     +----------------------------------------------------------
//!     |
//!     |   A formal methods enthusiast? Thanks for stopping by.
//!     |   While you are here, give us a star
//!     |
//!     |   ⭐ https://github.com/resonatehq/resonate
//!     |
//!     +----------------------------------------------------------

use vstd::prelude::*;

use crate::comparator::*;
use crate::proof::*;
use crate::spec::*;
use crate::timeout::Timeout;

verus! {

fn slot_for<T>(items: &Vec<Timeout<T>>, deadline: u64) -> (r: usize)
    requires
        sorted(items@),
    ensures
        r <= items@.len(),
        forall|j: int| 0 <= j < r ==> #[trigger] items@[j].deadline <= deadline,
        r < items@.len() ==> deadline < items@[r as int].deadline,
{
    let mut i: usize = 0;
    while i < items.len() && items[i].deadline <= deadline
        invariant
            i <= items@.len(),
            forall|j: int| 0 <= j < i ==> #[trigger] items@[j].deadline <= deadline,
        decreases items@.len() - i,
    {
        i = i + 1;
    }
    i
}

fn place<T>(items: &mut Vec<Timeout<T>>, t: Timeout<T>)
    requires
        sorted(old(items)@),
    ensures
        final(items)@ == spec_insert(old(items)@, t),
        sorted(final(items)@),
        final(items)@.len() == old(items)@.len() + 1,
{
    let ghost s0 = items@;
    let j = slot_for(items, t.deadline);
    proof {
        lemma_insert_at_is_spec_insert(s0, t, j as int);
    }
    items.insert(j, t);
    proof {
        lemma_spec_insert_sorted(s0, t);
        lemma_spec_insert_len(s0, t);
    }
}

fn mentioned<T, C: Comparator<T>>(cmp: &C, batch: &Vec<Timeout<T>>, v: &T) -> (r: bool)
    ensures
        r == mentions(*cmp, batch@, *v),
{
    let mut i: usize = 0;
    while i < batch.len()
        invariant
            i <= batch@.len(),
            forall|j: int| 0 <= j < i ==> !cmp.same(#[trigger] batch@[j].value, *v),
        decreases batch@.len() - i,
    {
        if cmp.eq(&batch[i].value, v) {
            return true;
        }
        i = i + 1;
    }
    false
}

fn drop_mentioned<T, C: Comparator<T>>(
    cmp: &C,
    items: Vec<Timeout<T>>,
    batch: &Vec<Timeout<T>>,
) -> (r: Vec<Timeout<T>>)
    ensures
        r@ == spec_untouched(*cmp, items@, batch@),
{
    let ghost s0 = items@;
    let mut rest = items;
    let mut out: Vec<Timeout<T>> = Vec::new();
    let n = rest.len();
    let mut k: usize = 0;

    while k < n
        invariant
            k <= n,
            n == s0.len(),
            rest@ == s0.skip(k as int),
            out@ + spec_untouched(*cmp, rest@, batch@) == spec_untouched(*cmp, s0, batch@),
        decreases n - k,
    {
        let ghost before = rest@;
        let t = rest.remove(0);
        proof {
            assert(rest@ =~= before.skip(1));
        }
        if !mentioned(cmp, batch, &t.value) {
            let ghost o = out@;
            out.push(t);
            proof {
                assert(o.push(t) + spec_untouched(*cmp, rest@, batch@) =~= o + (seq![t]
                    + spec_untouched(*cmp, rest@, batch@)));
            }
        }
        k = k + 1;
    }
    proof {
        assert(rest@ =~= Seq::<Timeout<T>>::empty());
        assert(out@ =~= spec_untouched(*cmp, s0, batch@));
    }
    out
}

fn last_updates<T, C: Comparator<T>>(cmp: &C, batch: Vec<Timeout<T>>) -> (r: Vec<Timeout<T>>)
    ensures
        r@ == spec_updates(*cmp, batch@),
{
    let ghost b0 = batch@;
    let mut rest = batch;
    let mut out: Vec<Timeout<T>> = Vec::new();
    let n = rest.len();
    let mut k: usize = 0;

    while k < n
        invariant
            k <= n,
            n == b0.len(),
            rest@ == b0.skip(k as int),
            out@ + spec_updates(*cmp, rest@) == spec_updates(*cmp, b0),
        decreases n - k,
    {
        let ghost before = rest@;
        let t = rest.remove(0);
        proof {
            assert(rest@ =~= before.skip(1));
        }
        if !mentioned(cmp, &rest, &t.value) {
            let ghost o = out@;
            out.push(t);
            proof {
                assert(o.push(t) + spec_updates(*cmp, rest@) =~= o + (seq![t] + spec_updates(
                    *cmp,
                    rest@,
                )));
            }
        }
        k = k + 1;
    }
    proof {
        assert(rest@ =~= Seq::<Timeout<T>>::empty());
        assert(out@ =~= spec_updates(*cmp, b0));
    }
    out
}

fn sort_by_deadline<T>(batch: Vec<Timeout<T>>) -> (r: Vec<Timeout<T>>)
    ensures
        r@ == spec_sort(batch@),
        sorted(r@),
        r@.len() == batch@.len(),
{
    let ghost inc0 = batch@;
    let mut src = batch;
    let mut out: Vec<Timeout<T>> = Vec::new();
    let n = src.len();
    let mut k: usize = 0;

    while k < n
        invariant
            k <= n,
            n == inc0.len(),
            src@ == inc0.skip(k as int),
            out@ == spec_sort_prefix(inc0, k as nat),
            sorted(out@),
        decreases n - k,
    {
        assert(src@[0] == inc0[k as int]);
        let t = src.remove(0);
        place(&mut out, t);
        proof {
            assert(src@ =~= inc0.skip(k as int + 1));
        }
        k = k + 1;
    }
    proof {
        lemma_spec_sort_wf(inc0, inc0.len());
    }
    out
}

pub struct TimerWheel<T, C: Comparator<T>> {
    cmp: C,
    cap: usize,
    items: Vec<Timeout<T>>,
}

impl<T, C: Comparator<T>> View for TimerWheel<T, C> {
    type V = Seq<Timeout<T>>;

    closed spec fn view(&self) -> Seq<Timeout<T>> {
        self.items@
    }
}

impl<T, C: Comparator<T>> TimerWheel<T, C> {
    pub closed spec fn comparator(&self) -> C {
        self.cmp
    }

    pub closed spec fn capacity_spec(&self) -> nat {
        self.cap as nat
    }

    pub open spec fn wf(&self) -> bool {
        &&& sorted(self@)
        &&& distinct(self.comparator(), self@)
        &&& self@.len() <= self.capacity_spec()
    }

    pub fn new(capacity: usize, cmp: C) -> (r: Self)
        ensures
            r.wf(),
            r@ == Seq::<Timeout<T>>::empty(),
            r.capacity_spec() == capacity,
            r.comparator() == cmp,
    {
        TimerWheel { cmp, cap: capacity, items: Vec::new() }
    }

    pub fn len(&self) -> (r: usize)
        ensures
            r == self@.len(),
    {
        self.items.len()
    }

    pub fn capacity(&self) -> (r: usize)
        ensures
            r == self.capacity_spec(),
    {
        self.cap
    }

    pub fn is_empty(&self) -> (r: bool)
        ensures
            r == (self@.len() == 0),
    {
        self.items.is_empty()
    }

    pub fn is_full(&self) -> (r: bool)
        requires
            self.wf(),
        ensures
            r == (self@.len() == self.capacity_spec()),
    {
        self.items.len() == self.cap
    }

    pub fn peek(&self) -> (r: Option<&Timeout<T>>)
        ensures
            self@.len() == 0 ==> r is None,
            self@.len() > 0 ==> r is Some && r->Some_0 == self@[0],
    {
        if self.items.len() == 0 {
            None
        } else {
            Some(&self.items[0])
        }
    }

    pub fn next(&self) -> (r: Option<u64>)
        requires
            self.wf(),
        ensures
            r is Some <==> self@.len() > 0,
            r matches Some(d) ==> d == self@[0].deadline,
            r matches Some(d) ==> forall|i: int|
                0 <= i < self@.len() ==> d <= #[trigger] self@[i].deadline,
    {
        if self.items.len() == 0 {
            None
        } else {
            Some(self.items[0].deadline)
        }
    }

    fn find_same(&self, v: &T) -> (r: usize)
        ensures
            r <= self@.len(),
            r < self@.len() ==> self.comparator().same(self@[r as int].value, *v),
            forall|j: int| 0 <= j < r ==> !self.comparator().same(#[trigger] self@[j].value, *v),
    {
        let mut i: usize = 0;
        while i < self.items.len()
            invariant
                i <= self.items@.len(),
                forall|j: int|
                    0 <= j < i ==> !self.cmp.same(#[trigger] self.items@[j].value, *v),
            decreases self.items@.len() - i,
        {
            if self.cmp.eq(&self.items[i].value, v) {
                return i;
            }
            i = i + 1;
        }
        i
    }

    pub fn merge(&mut self, incoming: Vec<Timeout<T>>)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            final(self).comparator() == old(self).comparator(),
            final(self).capacity_spec() == old(self).capacity_spec(),
            final(self)@ == spec_merge(
                old(self).comparator(),
                old(self)@,
                incoming@,
                old(self).capacity_spec(),
            ),
            old(self)@.len() <= final(self)@.len(),
            no_duplicates(old(self).comparator(), old(self)@)
                ==> no_duplicates(final(self).comparator(), final(self)@),
            forall|j: int|
                #![trigger incoming@[j].value]
                0 <= j < incoming@.len() && (forall|l: int|
                    j < l < incoming@.len() ==> !old(self).comparator().same(
                        #[trigger] incoming@[l].value,
                        incoming@[j].value,
                    )) ==> identity_carries(
                    final(self).comparator(),
                    final(self)@,
                    incoming@[j].value,
                    incoming@[j].deadline,
                ),
    {
        let ghost c = self.cmp;
        let ghost cap0 = self.cap;
        let ghost s0 = self.items@;
        let ghost inc0 = incoming@;

        let held = self.items.split_off(0);
        proof {
            assert(held@ =~= s0);
        }
        self.items = drop_mentioned(&self.cmp, held, &incoming);

        let mut updates = last_updates(&self.cmp, incoming);
        self.items.append(&mut updates);

        let ghost union = self.items@;
        let unsorted = self.items.split_off(0);
        proof {
            assert(unsorted@ =~= union);
        }
        self.items = sort_by_deadline(unsorted);

        let ghost ordered = self.items@;
        self.items.truncate(self.cap);

        proof {
            assert(union =~= spec_untouched(c, s0, inc0) + spec_updates(c, inc0));
            assert(ordered == spec_sort(union));
            assert(self.items@ =~= take_at_most(ordered, cap0 as nat));
            lemma_merge_wf(c, s0, inc0, cap0 as nat);
            lemma_no_duplicates_iff_distinct(c, s0);
            lemma_merge_preserves_no_duplicates(c, s0, inc0, cap0 as nat);
            assert forall|j: int|
                0 <= j < inc0.len() && (forall|l: int|
                    j < l < inc0.len() ==> !c.same(#[trigger] inc0[l].value, inc0[j].value))
                    implies identity_carries(c, self.items@, inc0[j].value, inc0[j].deadline) by {
                lemma_merge_sets_identity(c, s0, inc0, j, cap0 as nat);
            }
        }
    }

    pub fn insert(&mut self, t: Timeout<T>)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            final(self).comparator() == old(self).comparator(),
            final(self).capacity_spec() == old(self).capacity_spec(),
            final(self)@ == spec_merge(
                old(self).comparator(),
                old(self)@,
                seq![t],
                old(self).capacity_spec(),
            ),
            no_duplicates(old(self).comparator(), old(self)@)
                ==> no_duplicates(final(self).comparator(), final(self)@),
    {
        let mut batch = Vec::new();
        batch.push(t);
        proof {
            assert(batch@ =~= seq![t]);
        }
        self.merge(batch);
    }

    pub fn pop_expired(&mut self, now: u64) -> (r: Vec<Timeout<T>>)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            final(self).comparator() == old(self).comparator(),
            final(self).capacity_spec() == old(self).capacity_spec(),
            r@ + final(self)@ == old(self)@,
            forall|i: int| 0 <= i < r@.len() ==> #[trigger] r@[i].deadline <= now,
            forall|i: int| 0 <= i < final(self)@.len() ==> now < #[trigger] final(self)@[i].deadline,
            (old(self)@.len() > 0 && now < old(self)@[0].deadline) ==> r@.len() == 0,
    {
        let ghost c = self.cmp;
        let ghost s0 = self.items@;

        let k = slot_for(&self.items, now);

        let remaining = self.items.split_off(k);
        let expired = self.items.split_off(0);
        self.items = remaining;

        proof {
            lemma_suffix_sorted(s0, k as int);
            lemma_suffix_distinct(c, s0, k as int);
            assert(expired@ + self.items@ =~= s0);
            assert forall|i: int| 0 <= i < self.items@.len() implies now
                < self.items@[i].deadline by {
                assert(self.items@[i] == s0[k + i]);
                assert(s0[k as int].deadline <= s0[k + i].deadline);
            }
            if expired@.len() > 0 {
                assert(expired@[0] == s0[0]);
            }
        }
        expired
    }
}

} // verus!
