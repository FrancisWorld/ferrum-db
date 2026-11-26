//! Glob-style pattern matching for `KEYS`, `SCAN MATCH` and `PSUBSCRIBE`.
//!
//! Supported syntax: `*`, `?`, `[abc]`, `[a-z]`, `[^abc]` and `\` escapes.
//! Matching is iterative with single-star backtracking, so it is O(pattern x input)
//! in the worst case and never recurses.

/// Returns true when `input` matches `pattern`.
pub fn matches(pattern: &[u8], input: &[u8]) -> bool {
    let (mut p, mut i) = (0usize, 0usize);
    let mut star: Option<(usize, usize)> = None;
    while i < input.len() {
        let advanced = match pattern.get(p) {
            Some(b'*') => {
                star = Some((p, i));
                p += 1;
                continue;
            }
            Some(b'?') => Some((1, 1)),
            Some(b'[') => match class(&pattern[p..], input[i]) {
                Some((true, used)) => Some((used, 1)),
                Some((false, _)) => None,
                None => (input[i] == b'[').then_some((1, 1)),
            },
            Some(b'\\') if p + 1 < pattern.len() => (pattern[p + 1] == input[i]).then_some((2, 1)),
            Some(&c) => (c == input[i]).then_some((1, 1)),
            None => None,
        };
        match advanced {
            Some((dp, di)) => {
                p += dp;
                i += di;
            }
            None => match star {
                Some((sp, si)) => {
                    p = sp + 1;
                    i = si + 1;
                    star = Some((sp, si + 1));
                }
                None => return false,
            },
        }
    }
    while pattern.get(p) == Some(&b'*') {
        p += 1;
    }
    p == pattern.len()
}

/// Evaluates a `[...]` class at the start of `pat` against `c`.
/// Returns `(matched, bytes_consumed)`, or `None` when the class is unterminated.
fn class(pat: &[u8], c: u8) -> Option<(bool, usize)> {
    let mut i = 1;
    let negate = pat.get(i) == Some(&b'^');
    if negate {
        i += 1;
    }
    let mut matched = false;
    let mut first = true;
    loop {
        let &b = pat.get(i)?;
        if b == b']' && !first {
            return Some((matched != negate, i + 1));
        }
        first = false;
        let lo = if b == b'\\' {
            i += 1;
            *pat.get(i)?
        } else {
            b
        };
        if pat.get(i + 1) == Some(&b'-') && pat.get(i + 2).is_some_and(|&n| n != b']') {
            let mut hi = pat[i + 2];
            let mut next = i + 3;
            if hi == b'\\' {
                hi = *pat.get(i + 3)?;
                next = i + 4;
            }
            let (a, z) = if lo <= hi { (lo, hi) } else { (hi, lo) };
            matched |= (a..=z).contains(&c);
            i = next;
        } else {
            matched |= lo == c;
            i += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::matches;

    fn m(p: &str, s: &str) -> bool {
        matches(p.as_bytes(), s.as_bytes())
    }

    #[test]
    fn literals_and_wildcards() {
        assert!(m("hello", "hello"));
        assert!(!m("hello", "hell"));
        assert!(m("h?llo", "hallo"));
        assert!(m("*", ""));
        assert!(m("*", "anything"));
        assert!(m("h*o", "hello"));
        assert!(m("user:*:name", "user:42:name"));
        assert!(!m("user:*:name", "user:42:nam"));
        assert!(m("a*b*c", "aXXbYYc"));
        assert!(!m("a*b*c", "aXXbYY"));
    }

    #[test]
    fn classes() {
        assert!(m("h[ae]llo", "hello"));
        assert!(m("h[ae]llo", "hallo"));
        assert!(!m("h[ae]llo", "hillo"));
        assert!(m("h[^e]llo", "hallo"));
        assert!(!m("h[^e]llo", "hello"));
        assert!(m("h[a-b]llo", "hbllo"));
        assert!(m("[]]", "]"));
    }

    #[test]
    fn escapes_and_malformed_classes() {
        assert!(m(r"a\*b", "a*b"));
        assert!(!m(r"a\*b", "aXb"));
        assert!(m("[abc", "[abc"));
    }

    #[test]
    fn pathological_pattern_is_fast() {
        let input = "a".repeat(5000);
        assert!(!m(&format!("{}b", "a*".repeat(50)), &input));
    }
}
