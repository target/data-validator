if OS.mac?
  # We need a version of a JDK older than what's kept in the main cask repository.
  tap "homebrew/cask-versions"
  # The JDK formerly known as AdoptOpenJDK, a build of OpenJDK
  # Use JDK8 because that's a solid base for older Hadoop clusters
  # Also, Spark 2.3.x essentially requires Scala 2.11.x, and that combination may necessitate Java 8.
  cask "homebrew/cask-versions/temurin8"
end