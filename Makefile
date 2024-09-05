## Build the package documentation
docs-r:
	Rscript -e 'library(devtools);pkgload::load_all();styler::style_pkg();devtools::document();devtools::check(error_on="error")'


## Build the package website
docs-html:
	Rscript -e 'library(devtools);pkgload::load_all();styler::style_pkg();devtools::document();devtools::check(error_on="error")'
	Rscript -e 'devtools::install();pkgdown::build_site()'

## Build the package sources as .tar.gz
build:
	Rscript -e 'devtools::install();devtools::build()'
	mv ../sndsTools_* .

## Lint the package
lint:
	Rscript -e 'lintr::lint_package()'

## Test the package
test:
	Rscript -e 'devtools::test()'
	
styler: 
	Rscript -e 'styler::style_pkg()'