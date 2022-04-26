build-PandasPyarrow:
	mkdir -p "$(ARTIFACTS_DIR)/python"
	# The binary dist of numpy makes the Lambda package too big, so we'll build it ourselves
	CFLAGS='-g0 -Wl,--strip-all -I/usr/include:/usr/local/include -L/usr/lib:/usr/local/lib -DPYARROW_BUILD_PARQUET=on -DPYARROW_BUILD_S3=on' \
	pip install numpy --global-option=build_ext -t "$(ARTIFACTS_DIR)/python"
	pip install openpyxl pandas pyarrow -t "$(ARTIFACTS_DIR)/python"