require "../src/temporalio"

# Example activity that sends an email
class EmailActivity
  include Temporalio::Activity
  
  def execute(to : String, subject : String, body : String) : String
    # Simulate sending email
    puts "Sending email to #{to}: #{subject}"
    "Email sent successfully to #{to}"
  end
end

# Example activity that processes a number
class CalculateActivity
  include Temporalio::Activity
  
  def execute(x : Int32, y : Int32) : Int32
    x + y
  end
end

# Child workflow that processes data
class DataProcessingWorkflow
  include Temporalio::Workflow
  
  def execute(data : String) : String
    # Process the data
    processed = data.upcase
    puts "Child workflow processed: #{processed}"
    processed
  end
end

# Parent workflow demonstrating transparent payload conversion
class ParentWorkflow
  include Temporalio::Workflow
  
  def execute(user_email : String, user_name : String) : String
    ctx = Workflow::Context.current
    
    # ✅ Execute activity with automatic type conversion
    # No manual to_payload/from_payload needed!
    email_result = ctx.execute_activity(
      EmailActivity,
      user_email,                    # Regular String
      "Welcome!",                    # Regular String
      "Hello #{user_name}",          # Regular String
      task_queue: "email-queue",
      start_to_close_timeout: 30.seconds
    )
    # email_result is automatically String (inferred from EmailActivity#execute)
    
    puts "Activity result: #{email_result}"
    
    # ✅ Execute another activity with different types
    calc_result = ctx.execute_activity(
      CalculateActivity,
      10,                            # Regular Int32
      20,                            # Regular Int32
      task_queue: "calc-queue",
      start_to_close_timeout: 10.seconds
    )
    # calc_result is automatically Int32 (inferred from CalculateActivity#execute)
    
    puts "Calculation result: #{calc_result}"
    
    # ✅ Execute child workflow with automatic type conversion
    child_result = ctx.execute_child_workflow(
      DataProcessingWorkflow,
      "some data to process",        # Regular String
      task_queue: "processing-queue"
    )
    # child_result is automatically String (inferred from DataProcessingWorkflow#execute)
    
    puts "Child workflow result: #{child_result}"
    
    # ✅ All results are properly typed - compiler knows the types!
    "Workflow completed: #{email_result}, calc=#{calc_result}, child=#{child_result}"
  end
end

# This example shows:
# 1. No manual to_payload calls needed
# 2. No manual from_payload calls needed
# 3. Return types are automatically inferred from activity/workflow classes
# 4. Full compile-time type safety
# 5. IDE autocomplete works perfectly
#
# Compare this to the old way:
#
#   # OLD WAY (manual conversion) 😞
#   converter = Temporalio::DataConverter::DEFAULT
#   email_payload = ctx.execute_activity_internal(
#     "EmailActivity",
#     [converter.to_payload(user_email), converter.to_payload("Welcome!"), ...],
#     task_queue: "email-queue"
#   )
#   email_result = converter.from_payload(email_payload, String)
#
#   # NEW WAY (automatic) ✅
#   email_result = ctx.execute_activity(EmailActivity, user_email, "Welcome!", ..., task_queue: "email-queue")
