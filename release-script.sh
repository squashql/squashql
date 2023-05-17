#!/bin/bash
set -e

# If deploy fails because of:
# gpg: signing failed: Inappropriate ioctl for device. Source https://github.com/keybase/keybase-issues/issues/2798
export GPG_TTY=$(tty)

export TAG=1.1.1

read -p "Going to release version $TAG, are you sure? " -n 1 -r
echo    # (optional) move to a new line
if [[ $REPLY =~ ^[Yy]$ ]]
then
	git co -b release/$TAG
	# Set mvn version to match the tag
	mvn versions:set -DgenerateBackupPoms=false -DnewVersion=$TAG
	# Build the project and run the tests
	mvn clean install
	# Commit with updated poms
	git add .
	git commit -m "Update versions $TAG"
	# Create and push tags
	git tag -a $TAG -m "Create tag $TAG"
	git co $TAG
	# Push the tag
	git push origin $TAG
	# Deploy
	mvn clean deploy -Prelease -DskipTests
	echo "Tag $TAG pushed!"

	# Delete release branch
	git co main && git br -D release/$TAG
fi

# Once deploy, co main branch, bump version if needed

# DELETE TAG
# git co main && git br -D release/$TAG && git tag -d $TAG && git push --delete origin $TAG

## NPM

# for npm, execute:
# npm publish --access public

# To unpublish version '1.0.4-SNAPSHOT':
# npm unpublish @squashql/squashql-js@1.0.4-SNAPSHOT
