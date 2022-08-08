package csvwcheck

import io.cucumber.junit.{Cucumber, CucumberOptions}
import org.junit.runner.RunWith

@RunWith(classOf[Cucumber])
@CucumberOptions(
  features = Array("src/test/resources/features"),
  tags = "@runme" // "not @ignore"
)
//@CucumberOptions(tags = "@runThisTest")
class RunCucumberTest
