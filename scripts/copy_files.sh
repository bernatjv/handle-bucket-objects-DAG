ALL_ARGUMENTS=($@)
FILES=(${ALL_ARGUMENTS[@]:1})
BUCKET="$1"
for file in ${FILES[@]};
	do
		gsutil cp gs://$BUCKET$file gs://$BUCKET/data/copy$file
	done
