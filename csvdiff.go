package main

import (
	"fmt"
	"github.com/rickdana/csvdiff/cmd"
	"github.com/rickdana/csvdiff/pkg/digest"
	"github.com/spf13/afero"
)

type CsvDiff struct {
	primaryKeyPositions        []int
	valueColumnPositions       []int
	ignoreValueColumnPositions []int
	includeColumnPositions     []int
	format                     string
	separator                  string
	lazyQuotes                 bool
}

func NewCsvDiff(primaryKeyPositions []int, valueColumnPositions []int, ignoreValueColumnPositions []int, includeColumnPositions []int, format string, separator string, lazyQuotes bool) *CsvDiff {
	return &CsvDiff{primaryKeyPositions: primaryKeyPositions, valueColumnPositions: valueColumnPositions, ignoreValueColumnPositions: ignoreValueColumnPositions, includeColumnPositions: includeColumnPositions, format: format, separator: separator, lazyQuotes: lazyQuotes}
}

func (cd *CsvDiff) Compare(baseFile, deltaFile string) (*digest.Differences, error) {
	fs := afero.NewOsFs()
	runSeparator, err := cmd.ParseSeparator(cd.separator)
	if err != nil {
		return nil, err
	}

	ctx, err := cmd.NewContext(
		fs,
		cd.primaryKeyPositions,
		cd.valueColumnPositions,
		cd.ignoreValueColumnPositions,
		cd.includeColumnPositions,
		cd.format,
		baseFile,
		deltaFile,
		runSeparator,
		cd.lazyQuotes,
	)

	if err != nil {
		return nil, err
	}
	defer ctx.Close()

	baseConfig, err := ctx.BaseDigestConfig()
	if err != nil {
		return nil, fmt.Errorf("error opening base-file %s: %v", ctx.BaseFilename, err)
	}
	deltaConfig, err := ctx.DeltaDigestConfig()
	if err != nil {
		return nil, fmt.Errorf("error opening delta-file %s: %v", ctx.DeltaFilename, err)
	}
	defer ctx.Close()

	diff, err := digest.Diff(baseConfig, deltaConfig)
	return &diff, err
}
