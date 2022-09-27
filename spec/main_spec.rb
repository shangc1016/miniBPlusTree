

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



  # # 1、
  # it 'inserts and retrieves a row' do
  #   result = run_script([
  #     "insert 1 user1 person1@example.com",
  #     "select",
  #     ".exit",
  #   ])

  #   expect(result).to match_array([
  #     "db > Executed.",
  #     "db > (1, user1, person1@example.com)",
  #     "Executed.",
  #     "db > ",
  #   ])
  # end


  # # 2、
  # it 'prints error message when table is full' do
  #   script = (1..10).map do |i|
  #     "insert #{i} user#{i} person#{i}@example.com"
  #   end
  #   script << ".exit"
  #   result = run_script(script)
  #   # expect(result[-2]).to eq('db > Error: Table full.')
  # end


  # # 3
  # it 'allows inserting strings that are the maximum length' do 
  #   long_username = "a"*32
  #   long_email = "b"*255

  #   script = [
  #     "insert 1 #{long_username} #{long_email}",
  #     "select",
  #     ".exit"
  #   ]

  #   result = run_script(script)
  #   # puts result
  #   expect(result).to match_array([
  #     "db > Executed.",
  #     "db > (1, #{long_username}, #{long_email})",
  #     "Executed.",
  #     "db > ",
  #   ])
  # end


  # # 4
  # it 'prints error message if strings are too long' do
  #   long_username = "a"*33
  #   long_email = "a"*256
  #   script = [
  #     "insert 1 #{long_username} #{long_email}",
  #     "select",
  #     ".exit",
  #   ]

  #   result = run_script(script)
  #   expect(result).to match_array([
  #     "db > String is too long.",
  #     "db > Executed.",
  #     "db > ",
  #   ])
  # end


  # # 5
  # it 'prints an error message if id is nagative' do
  #   script = [
  #     "insert -1 shangc shang@chao.com",
  #     "selectg",
  #     ".exit"
  #   ]
  #   result = run_script(script)
  #   expect(result).to match_array([
  #     "db > ID must be positive.",
  #     "db > Executed.",
  #     "db > "
  #   ])
  # end


  # # part5 new 6
  # it 'keeps data after closing connection' do
  #   result1 = run_script([
  #     "insert 1 user1 person1@example.com",
  #     ".exit",
  #   ])
  #   expect(result1).to match_array([
  #     "db > Executed.",
  #     "db > "
  #   ])

  #   result2 = run_script([
  #     "db > (1, user1, person1@example.com)",
  #     "Executed.",
  #     "db > "
  #   ])
  # end

  # # next test script here.

  # # part9 test case, dup key forbid.
  # it 'prints an error message if there is a dublicate key' do
  #   script = [
  #     "insert 1 user1 person1@example.com",
  #     "insert 1 user2 person1@example.com",
  #     "select",
  #     ".exit"
  #   ]

  #   result = run_script(script)

  #   expect(result).to match_array([
  #     "db > Executed.",
  #     "db > Error: Duplicate key.",
  #     "db > (1, user1, person1@example.com)",
  #     "Executed.",
  #     "db > "
  #   ])
  # end


  # part11  
  # it 'allows printing out the structure of 3-level-node-btree' do
  #   script = (1..140).map do | i |
  #   "insert #{i} user#{i} person#{i}@example.com"
  #   end
  
  #   script << ".btree"
  #   script << "insert 15 user15 person15@example.com"
  #   script << ".exit"
  #   result = run_script(script)

  #   puts result

  #   # expect(result[14...(result.length)]).to match_array([
  #   #   "db > Tree:",
  #   #   "- internal (size 1)",
  #   #   "  - leaf (size 7)",
  #   #   "    - 1",
  #   #   "    - 2",
  #   #   "    - 3",
  #   #   "    - 4",
  #   #   "    - 5",
  #   #   "    - 6",
  #   #   "    - 7",
  #   #   "  - key 7",
  #   #   "  - leaf (size 7)",
  #   #   "    - 8",
  #   #   "    - 9",
  #   #   "    - 10",
  #   #   "    - 11",
  #   #   "    - 12",
  #   #   "    - 13",
  #   #   "    - 14",
  #   #   "db > Executed.",
  #   #   "db > "
  #   # ])
  # end


  # part13
  it 'allows printing out the structure of a 34-level-node btree' do
    script = [
      "insert 18 user18 person18@example.com",
      "insert 7 user7 person7@example.com",
      "insert 10 user10 person10@example.com",
      "insert 29 user29 person29@example.com",
      "insert 23 user23 person23@example.com",
      "insert 4 user4 person4@example.com",
      "insert 14 user14 person14@example.com",
      "insert 30 user30 person30@example.com",
      "insert 15 user15 person15@example.com",
      "insert 26 user26 person26@example.com",
      "insert 22 user22 person22@example.com",
      "insert 19 user19 person19@example.com",
      "insert 2 user2 person2@example.com",
      "insert 1 user1 person1@example.com",
      "insert 21 user21 person21@example.com",
      "insert 11 user11 person11@example.com",
      "insert 6 user6 person6@example.com",
      "insert 20 user20 person20@example.com",
      "insert 5 user5 person5@example.com",
      "insert 8 user8 person8@example.com",
      "insert 9 user9 person9@example.com",
      "insert 3 user3 person3@example.com",
      "insert 12 user12 person12@example.com",
      "insert 27 user27 person27@example.com",
      "insert 17 user17 person17@example.com",
      "insert 16 user16 person16@example.com",
      "insert 13 user13 person13@example.com",
      "insert 24 user24 person24@example.com",
      "insert 25 user25 person25@example.com",
      "insert 28 user28 person28@example.com",
      ".btree",
      ".exit",
    ]
    result = run_script(script)

    puts result

    # expect(result[30...(result.length)]).to eq([
    #   "db > Tree:",
    #   "- internal (size 3)",
    #   "  - leaf (size 7)",
    #   "    - 1",
    #   "    - 2",
    #   "    - 3",
    #   "    - 4",
    #   "    - 5",
    #   "    - 6",
    #   "    - 7",
    #   "  - key 7",
    #   "  - leaf (size 8)",
    #   "    - 8",
    #   "    - 9",
    #   "    - 10",
    #   "    - 11",
    #   "    - 12",
    #   "    - 13",
    #   "    - 14",
    #   "    - 15",
    #   "  - key 15",
    #   "  - leaf (size 7)",
    #   "    - 16",
    #   "    - 17",
    #   "    - 18",
    #   "    - 19",
    #   "    - 20",
    #   "    - 21",
    #   "    - 22",
    #   "  - key 22",
    #   "  - leaf (size 8)",
    #   "    - 23",
    #   "    - 24",
    #   "    - 25",
    #   "    - 26",
    #   "    - 27",
    #   "    - 28",
    #   "    - 29",
    #   "    - 30",
    #   "db > ",
    # ])

    end

   


end
