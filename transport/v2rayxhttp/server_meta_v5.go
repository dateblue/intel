//go:build with_xhttp

package v2rayxhttp

import (
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/sagernet/sing-box/option"
	"golang.org/x/net/http2/hpack"
)

type serverMetaV5 struct {
	sessionPlacement, sessionKey                               string
	seqPlacement, seqKey                                       string
	uplinkPlacement, uplinkKey                                 string
	paddingObfs                                                bool
	paddingPlacement, paddingKey, paddingHeader, paddingMethod string
	maxPost                                                    int
}

func newServerMetaV5(o option.V2RayXHTTPOptions) (serverMetaV5, error) {
	maxPost, err := parseRangeOr(o.ScMaxEachPostBytes, "sc_max_each_post_bytes", intRange{1000000, 1000000})
	if err != nil {
		return serverMetaV5{}, err
	}
	m := serverMetaV5{
		sessionPlacement: orDefault(o.SessionPlacement, placementPath),
		seqPlacement:     orDefault(o.SeqPlacement, placementPath),
		uplinkPlacement:  orDefault(o.UplinkDataPlacement, placementBody),
		paddingObfs:      o.XPaddingObfsMode,
		paddingPlacement: orDefault(o.XPaddingPlacement, placementQueryInHeader),
		paddingKey:       orDefault(o.XPaddingKey, "x_padding"),
		paddingHeader:    orDefault(o.XPaddingHeader, "X-Padding"),
		paddingMethod:    orDefault(o.XPaddingMethod, methodRepeatX),
		maxPost:          maxPost.max,
	}
	m.sessionKey = resolveKey(o.SessionKey, m.sessionPlacement, "X-Session", "x_session")
	m.seqKey = resolveKey(o.SeqKey, m.seqPlacement, "X-Seq", "x_seq")
	m.uplinkKey = resolveUplinkDataKey(o.UplinkDataKey, m.uplinkPlacement)
	for _, check := range []struct {
		field, value string
		allowed      []string
	}{
		{"session_placement", m.sessionPlacement, []string{placementPath, placementQuery, placementHeader, placementCookie}},
		{"seq_placement", m.seqPlacement, []string{placementPath, placementQuery, placementHeader, placementCookie}},
		{"uplink_data_placement", m.uplinkPlacement, []string{placementAuto, placementBody, placementHeader, placementCookie}},
		{"x_padding_placement", m.paddingPlacement, []string{placementCookie, placementHeader, placementQuery, placementQueryInHeader}},
		{"x_padding_method", m.paddingMethod, []string{methodRepeatX, methodTokenish}},
	} {
		if err := validatePlacement(check.field, check.value, check.allowed...); err != nil {
			return serverMetaV5{}, err
		}
	}
	if m.maxPost <= 0 || m.maxPost > 32*1024*1024 {
		return serverMetaV5{}, fmt.Errorf("v2ray-xhttp: sc_max_each_post_bytes must be in 1..33554432")
	}
	return m, nil
}

func serverMetaField(r *http.Request, placement, key string, pathSegment string) string {
	switch placement {
	case placementPath:
		return pathSegment
	case placementQuery:
		return r.URL.Query().Get(key)
	case placementHeader:
		return r.Header.Get(key)
	case placementCookie:
		cookie, e := r.Cookie(key)
		if e == nil {
			return cookie.Value
		}
	}
	return ""
}

func (m serverMetaV5) extract(r *http.Request, basePath string) (session, seq string, ok bool) {
	if !strings.HasPrefix(r.URL.Path, basePath) {
		return "", "", false
	}
	var subpath []string
	if m.sessionPlacement == placementPath || m.seqPlacement == placementPath {
		suffix := strings.TrimPrefix(r.URL.Path, basePath)
		if suffix != "" {
			subpath = strings.Split(suffix, "/")
		}
	}
	part := 0
	var sPart, qPart string
	if m.sessionPlacement == placementPath && part < len(subpath) {
		sPart = subpath[part]
		part++
	}
	if m.seqPlacement == placementPath && part < len(subpath) {
		qPart = subpath[part]
		part++
	}
	if part < len(subpath) {
		return "", "", false
	}
	session = serverMetaField(r, m.sessionPlacement, m.sessionKey, sPart)
	seq = serverMetaField(r, m.seqPlacement, m.seqKey, qPart)
	if len(session) > 256 || len(seq) > 32 {
		return "", "", false
	}
	return session, seq, true
}

func (m serverMetaV5) requestPadding(r *http.Request) string {
	if !m.paddingObfs {
		if ref := r.Header.Get("Referer"); ref != "" {
			u, e := url.Parse(ref)
			if e != nil {
				return ""
			}
			return u.Query().Get("x_padding")
		}
		return r.URL.Query().Get("x_padding")
	}
	if c, e := r.Cookie(m.paddingKey); e == nil && c.Value != "" {
		return c.Value
	}
	if head := r.Header.Get(m.paddingHeader); head != "" {
		if m.paddingPlacement == placementHeader {
			return head
		}
		if u, e := url.Parse(head); e == nil {
			return u.Query().Get(m.paddingKey)
		}
	}
	return r.URL.Query().Get(m.paddingKey)
}

func (m serverMetaV5) paddingValid(r *http.Request, allowed intRange) bool {
	value := m.requestPadding(r)
	if value == "" {
		return false
	}
	n := len(value)
	if m.paddingObfs && m.paddingMethod == methodTokenish {
		n = int(hpack.HuffmanEncodeLength(value))
		return n >= allowed.min-2 && n <= allowed.max+2
	}
	return n >= allowed.min && n <= allowed.max
}

func (m serverMetaV5) responsePadding(w http.ResponseWriter, n int) {
	if n <= 0 {
		return
	}
	text := strings.Repeat("X", n)
	if m.paddingObfs && m.paddingMethod == methodTokenish {
		text = generateTokenishPaddingBase62(n)
	}
	if m.paddingObfs {
		switch m.paddingPlacement {
		case placementHeader:
			w.Header().Set(m.paddingHeader, text)
		case placementQueryInHeader:
			u := url.URL{Scheme: "https", Host: "localhost", Path: "/"}
			u.RawQuery = m.paddingKey + "=" + text
			w.Header().Set(m.paddingHeader, u.String())
		case placementCookie:
			http.SetCookie(w, &http.Cookie{Name: m.paddingKey, Value: text, Path: "/"})
		}
	} else {
		w.Header().Set("X-Padding", text)
	}
}

func (m serverMetaV5) payload(r *http.Request) ([]byte, error) {
	var payload []byte
	if m.uplinkPlacement == placementHeader || m.uplinkPlacement == placementAuto {
		var chunks strings.Builder
		for i := 0; i < 4096; i++ {
			chunk := r.Header.Get(fmt.Sprintf("%s-%d", m.uplinkKey, i))
			if chunk == "" {
				break
			}
			if chunks.Len()+len(chunk) > m.maxPost*2 {
				return nil, fmt.Errorf("header upload too large")
			}
			chunks.WriteString(chunk)
		}
		if chunks.Len() > 0 {
			b, e := base64.RawURLEncoding.DecodeString(chunks.String())
			if e != nil {
				return nil, e
			}
			payload = append(payload, b...)
		}
	}
	if m.uplinkPlacement == placementCookie || m.uplinkPlacement == placementAuto {
		var chunks strings.Builder
		for i := 0; i < 4096; i++ {
			c, e := r.Cookie(fmt.Sprintf("%s_%d", m.uplinkKey, i))
			if e != nil {
				break
			}
			if chunks.Len()+len(c.Value) > m.maxPost*2 {
				return nil, fmt.Errorf("cookie upload too large")
			}
			chunks.WriteString(c.Value)
		}
		if chunks.Len() > 0 {
			b, e := base64.RawURLEncoding.DecodeString(chunks.String())
			if e != nil {
				return nil, e
			}
			payload = append(payload, b...)
		}
	}
	if m.uplinkPlacement == placementAuto || m.uplinkPlacement == placementBody {
		if r.ContentLength > int64(m.maxPost) {
			return nil, fmt.Errorf("upload too large")
		}
		b, e := io.ReadAll(io.LimitReader(r.Body, int64(m.maxPost+1)))
		if e != nil {
			return nil, e
		}
		payload = append(payload, b...)
	}
	if len(payload) > m.maxPost {
		return nil, fmt.Errorf("upload too large")
	}
	return payload, nil
}

func (m serverMetaV5) uploadSeq(s string) (uint64, error) { return strconv.ParseUint(s, 10, 64) }
