package util

func CompactFunc[S ~[]E, E any](s S, eq func(*E, *E) bool) S {
	if len(s) < 2 {
		return s
	}
	i := 1
	for k := 1; k < len(s); k++ {
		if !eq(&s[k], &s[k-1]) {
			if i != k {
				s[i] = s[k]
			}
			i++
		}
	}
	clear(s[i:]) // zero/nil out the obsolete elements, for GC
	return s[:i]
}
