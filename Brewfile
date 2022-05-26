if OS.mac?
  # We need a version of a JDK older than what's kept in the main cask repository.
  tap "homebrew/cask-versions"
  # The JDK formerly known as AdoptOpenJDK, a build of OpenJDK
  # Use JDK8 because that's a solid base for older Hadoop clusters
  cask "homebrew/cask-versions/temurin8"
end