package wkafka

import (
	"bytes"
	"text/template"
)

var goTemplate = template.New("wkafka")

func templateRun(txt string, data any) (string, error) {
	tmpl, err := goTemplate.Parse(txt)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", err
	}

	return buf.String(), nil
}
