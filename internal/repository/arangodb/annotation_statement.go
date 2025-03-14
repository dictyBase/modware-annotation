// PickStatementResult is a struct that holds the result of pickStatement
// function.
type PickStatementResult struct {
	Statement string
	Err       error
}
// generateStatementForBothFilters creates a statement when both filter sets are
// non-empty.
func generateStatementForBothFilters[A, B []*query.Filter](
	first A,
	second B,
) PickStatementResult {
	var result PickStatementResult
	fmap := FilterMap()

	afilter, err := query.GenQualifiedAQLFilterStatement(fmap, first)
	if err != nil {
		result.Err = err

		return result
	}

	cfilter, err := query.GenQualifiedAQLFilterStatement(fmap, second)
	if err != nil {
		result.Err = err

		return result
	}
	result.Statement = fmt.Sprintf(annCvtListFilterQ, afilter, cfilter)

	return result
}

// generateStatementForFirstFilter creates a statement when only first filter
// set is non-empty.
func generateStatementForFirstFilter[A []*query.Filter](
	first A,
) PickStatementResult {
	var result PickStatementResult
	fmap := FilterMap()

	afilter, err := query.GenQualifiedAQLFilterStatement(fmap, first)
	if err != nil {
		result.Err = err

		return result
	}

	result.Statement = fmt.Sprintf(annExclusiveListFilterQ, afilter)

	return result
}

// generateStatementForSecondFilter creates a statement when only second filter
// set is non-empty.
func generateStatementForSecondFilter[B []*query.Filter](
	second B,
) PickStatementResult {
	var result PickStatementResult
	fmap := FilterMap()

	cfilter, err := query.GenQualifiedAQLFilterStatement(fmap, second)
	if err != nil {
		result.Err = err

		return result
	}

	result.Statement = fmt.Sprintf(cvtExclusiveListFilterQ, cfilter)

	return result
}
