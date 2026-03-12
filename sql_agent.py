import os
from typing import TypedDict
from dotenv import load_dotenv

# IMPORTANT: Ensure these specific imports are used
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_community.utilities import SQLDatabase
from langgraph.graph import StateGraph, END

load_dotenv()

# 1. Initialize Gemini
# Use gemini-1.5-flash for speed or gemini-1.5-pro for complex reasoning
llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash", temperature=0)

# 2. Connect to our Mock DB
# We use the absolute path to ensure it finds finance.db in your folder
db_path = os.path.abspath("finance.db")
db = SQLDatabase.from_uri(f"sqlite:///{db_path}")

# 3. Define the "State"
class AgentState(TypedDict):
    question: str
    sql_query: str
    db_results: str
    error: str
    iterations: int

# --- NODE 1: The SQL Generator ---
def generate_sql(state: AgentState):
    print(f"--- GENERATING SQL (Attempt {state.get('iterations', 0) + 1}) ---")
    
    prompt = f"""
    You are a financial data expert. Based on the schema below, write a SQL query.
    
    Schema:
    {db.get_table_info()}
    
    Question: {state['question']}
    Previous Error (if any): {state.get('error', 'None')}
    
    CRITICAL: ONLY return the raw SQL. Do not include markdown tags like ```sql.
    """
    
    response = llm.invoke(prompt)
    # Cleaning the response just in case the LLM includes markdown
    clean_sql = response.content.replace("```sql", "").replace("```", "").strip()
    
    return {
        "sql_query": clean_sql, 
        "iterations": state.get('iterations', 0) + 1
    }

# --- NODE 2: The Executor ---
def execute_sql(state: AgentState):
    print(f"--- EXECUTING SQL: {state['sql_query']} ---")
    try:
        result = db.run(state['sql_query'])
        print(f"--- SUCCESS: {result} ---")
        return {"db_results": str(result), "error": ""}
    except Exception as e:
        print(f"--- ERROR: {str(e)} ---")
        return {"error": str(e)}

# --- NODE: Result Validator ---
def validate_results(state: AgentState):
    print(f"--- VALIDATING RESULTS: {state['db_results']} ---")
    
    # If the result is None or empty, it's likely a logic error in the WHERE clause
    if state['db_results'] == "[(None,)]" or state['db_results'] == "[]":
        return {
            "error": (
                "The query returned no results. This usually means the WHERE clause "
                "filters are too restrictive or using the wrong column values. "
                "Please double-check the actual data values in the tables."
            )
        }
    return {"error": ""}


# --- UPDATED LOGIC ---
def should_continue(state: AgentState):
    # If there is an error (either SQL error or our Validation error)
    # AND we haven't exhausted our retries, try again.
    if state['error'] != "" and state['iterations'] < 3:
        print(f"--- RETRYING: {state['error']} ---")
        return "generate"
    return "end"



# 4. Build the Graph
workflow = StateGraph(AgentState)

workflow.add_node("generate", generate_sql)
workflow.add_node("execute", execute_sql)
workflow.add_node("validate", validate_results)

workflow.set_entry_point("generate")
workflow.add_edge("generate", "execute")
workflow.add_edge("execute", "validate") # Check the result after execution
workflow.add_conditional_edges("validate", should_continue, {"end": END, "generate": "generate"})

app = workflow.compile()

# Test it!
if __name__ == "__main__":
    # Let's try a question that requires joining tables or specific filters
    inputs = {
    "question": "Find the total balance of accounts belonging to VIP clients. Look at the metadata to find who is a VIP."
}

    for output in app.stream(inputs):
        pass # The print statements inside the nodes will show us the progress