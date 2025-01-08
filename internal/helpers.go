package internal

import "strings"

func SplitCompositeTypes(name string) []string {
	if !strings.Contains(name, "<") {
		return strings.Split(name, ", ")
	}
	var parts []string
	lessCount := 0
	segment := ""
	for _, char := range name {
		if char == ',' && lessCount == 0 {
			if segment != "" {
				parts = append(parts, strings.TrimSpace(segment))
			}
			segment = ""
			continue
		}
		segment += string(char)
		if char == '<' {
			lessCount++
		} else if char == '>' {
			lessCount--
		}
	}
	if segment != "" {
		parts = append(parts, strings.TrimSpace(segment))
	}
	return parts
}

func CopyBytes(p []byte) []byte {
	b := make([]byte, len(p))
	copy(b, p)
	return b
}
