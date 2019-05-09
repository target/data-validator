workflow "PullRequest" {
  on = "pull_request"
  resolves = ["compile data-validator"]
}

workflow "Push"  {
  on = "push"
  resolves = [ "compile data-validator" ]
}

action "compile data-validator" {
  uses = "docker://amazoncorretto:8"
  runs = "bin/sbt"
  args = "clean compile test package"
}

