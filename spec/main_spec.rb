

describe 'database' do

  before do
    `rm -rf src/test.db`
  end

  def run_script(commands)
    raw_output = nil
    IO.popen("./src/db src/test.db", "r+") do |pipe|
      commands.each do |command|
        # pipe.puts command
        begin
          pipe.puts command
        rescue Errno::EPIPE
          break
        end
      end

      pipe.close_write

      # Read entire output
      raw_output = pipe.gets(nil)
    end
    raw_output.split("\n")
  end



  # 1、
  it 'inserts and retrieves a row' do
    result = run_script([
      "insert 1 user1 person1@example.com",
      "select",
      ".exit",
    ])

    expect(result).to match_array([
      "db > Executed.",
      "db > (1, user1, person1@example.com)",
      "Executed.",
      "db > ",
    ])
  end


  # 2、
  it 'prints error message when table is full' do
    script = (1..10).map do |i|
      "insert #{i} user#{i} person#{i}@example.com"
    end
    script << ".exit"
    result = run_script(script)
    # expect(result[-2]).to eq('db > Error: Table full.')
  end


  # 3
  it 'allows inserting strings that are the maximum length' do 
    long_username = "a"*32
    long_email = "b"*255

    script = [
      "insert 1 #{long_username} #{long_email}",
      "select",
      ".exit"
    ]

    result = run_script(script)
    # puts result
    expect(result).to match_array([
      "db > Executed.",
      "db > (1, #{long_username}, #{long_email})",
      "Executed.",
      "db > ",
    ])
  end


  # 4
  it 'prints error message if strings are too long' do
    long_username = "a"*33
    long_email = "a"*256
    script = [
      "insert 1 #{long_username} #{long_email}",
      "select",
      ".exit",
    ]

    result = run_script(script)
    expect(result).to match_array([
      "db > String is too long.",
      "db > Executed.",
      "db > ",
    ])
  end


  # 5
  it 'prints an error message if id is nagative' do
    script = [
      "insert -1 shangc shang@chao.com",
      "selectg",
      ".exit"
    ]
    result = run_script(script)
    expect(result).to match_array([
      "db > ID must be positive.",
      "db > Executed.",
      "db > "
    ])
  end


  # part5 new 6
  it 'keeps data after closing connection' do
    result1 = run_script([
      "insert 1 user1 person1@example.com",
      ".exit",
    ])
    expect(result1).to match_array([
      "db > Executed.",
      "db > "
    ])

    result2 = run_script([
      "db > (1, user1, person1@example.com)",
      "Executed.",
      "db > "
    ])
  end

  # next test script here.

  # part9 test case, dup key forbid.
  it 'prints an error message if there is a dublicate key' do
    script = [
      "insert 1 user1 person1@example.com",
      "insert 1 user2 person1@example.com",
      "select",
      ".exit"
    ]

    result = run_script(script)

    expect(result).to match_array([
      "db > Executed.",
      "db > Error: Duplicate key.",
      "db > (1, user1, person1@example.com)",
      "Executed.",
      "db > "
    ])
  end


  # part11  
  it 'allows printing out the structure of 3-level-node-btree' do
    script = (1..14).map do | i |
    "insert #{i} user#{i} person#{i}@example.com"
    end
  
    script << ".btree"
    script << "insert 15 user15 person15@example.com"
    script << ".exit"
    result = run_script(script)

    # puts result

    expect(result[14...(result.length)]).to match_array([
      "db > Tree:",
      "- internal (size 1)",
      "  - leaf (size 7)",
      "    - 1",
      "    - 2",
      "    - 3",
      "    - 4",
      "    - 5",
      "    - 6",
      "    - 7",
      "  - key 7",
      "  - leaf (size 7)",
      "    - 8",
      "    - 9",
      "    - 10",
      "    - 11",
      "    - 12",
      "    - 13",
      "    - 14",
      "db > Executed.",
      "db > "
    ])
  end


end
