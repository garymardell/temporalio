class HelloActivity
  include Temporalio::Activity

  activity_name "HelloActivity"

  def execute(name : String) : String
    "Hello, #{name}!"
  end
end
