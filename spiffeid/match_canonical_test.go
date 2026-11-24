package spiffeid

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMatchPathPrefixRequiresCanonicalSegmentBoundary(t *testing.T) {
	testCases := []struct {
		name     string
		actual   string
		expected string
		matches  bool
	}{
		{
			name:     "exact match",
			actual:   "/A",
			expected: "/A",
			matches:  true,
		},
		{
			name:     "segment boundary",
			actual:   "/A/B",
			expected: "/A",
			matches:  true,
		},
		{
			name:     "prefix without segment boundary",
			actual:   "/AA",
			expected: "/A",
			matches:  false,
		},
		{
			name:     "trailing slash on actual",
			actual:   "/A/",
			expected: "/A",
			matches:  false,
		},
		{
			name:     "trailing slash on expected",
			actual:   "/A/B",
			expected: "/A/",
			matches:  false,
		},
		{
			name:     "empty segment",
			actual:   "/A//B",
			expected: "/A",
			matches:  false,
		},
		{
			name:     "dot segment",
			actual:   "/A/../B",
			expected: "/A",
			matches:  false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.matches, matchPathPrefix(testCase.actual, testCase.expected))
		})
	}
}

func TestMatchIDPrefixRejectsNonCanonicalPath(t *testing.T) {
	matcher := MatchIDPrefix(RequireFromString("spiffe://foo.test/A"))

	// Construct an ID with a non-canonical path, as could be held by a value
	// produced by an older version of the library.
	actual := ID{
		id:      "spiffe://foo.test/A/",
		pathidx: len("spiffe://foo.test"),
	}

	assert.EqualError(t, matcher(actual), `unexpected ID "spiffe://foo.test/A/"`)
}
